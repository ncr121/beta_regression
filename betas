import os
import time
import datetime as dt
import numpy as np
import pandas as pd
import statsmodels.api as sm
import xlwings as xw

import xlwings_functions as xwfn

pd.options.mode.chained_assignment = None


def return_latest_weights(conn, query):
    conn.add_static_query(query)
    df = conn.get_static_result('res')
    return df


def is_business_today():
    """Determine if it is a business day."""
    date = dt.date.today()
    return bool(len(pd.bdate_range(date, date)))


def get_pos_df(conn, name, table, override=False):
    query = '''res = db.t(name, table).where("Date={}")
               .lastBy("Date", "TradingGroup", "Sym")'''.format('currentDateNy()' if is_business_today() and not override else 'lastBusinessDateNy()')
    df = return_latest_weights(conn, query)

    return df[df['Quantity'] != 0]


def get_security_df(conn, name, table, date):
    query = '''res = db.t(name, table).where("Date>='{}'")'''.format(date)
    df = return_latest_weights(conn, query)
    df['Date'] = df['Date'].apply(lambda x: dt.datetime.strptime(x, '%Y-%m-%d').date())

    return df


def get_factor_values(factor, security_df):
    df = security_df[(security_df['SYM'] == factor) & (security_df['Factor'] == 'PX_LAST')].drop_duplicates('Date').set_index('Date')
    return df['FactorValue']


def compute_corr(x, y, days):
    """Compute the correlation between two series on overlapping days."""
    idx = sorted(set(x.index) & set(y.index))[-days:]
    if len(idx) < days:
        return
    else:
        return np.corrcoef(x[idx], y[idx])[0, 1]


def compute_beta(x, y, days):
    """Compute the beta between two series on overlapping days using OLS."""
    idx = sorted(set(x.index) & set(y.index))[-days:]
    if len(idx) < days:
        return
    else:
        model = sm.OLS(y[idx], x[idx]).fit()
        return model.params[0]


def compute_beta_adj_nmv(betas, nmv_agg):
    beta_adj = betas * nmv_agg / 1e6
    beta_adj.loc['Total'] = beta_adj.sum(axis=0)
    return beta_adj


def compute_all_x_and_y(X, Y, days, compute_fn):
    return pd.DataFrame({x_factor: {y_factor: compute_fn(x.dropna(), y.dropna(), days) for y_factor, y in Y.items()}
                         for x_factor, x in X.items()})


def agg_nmv(nmv, ascending=True):
    return nmv.groupby(nmv.index).sum().sort_values(ascending=ascending).to_frame('Unadjusted NMV')


def compute_spread_adj_nmv(df, denom):
    unadj_nmv = agg_nmv(df['NMV'])
    df = df.reindex(unadj_nmv.index)
    df['Spread Ratio'] = df['YLD/CDS Spread'] / denom
    df['Spread Adj NMV'] = unadj_nmv.squeeze() * df['Spread Ratio']
    spread_adj_nmv = df[['Spread Adj NMV', 'Spread Ratio', 'YLD/CDS Spread']]
    spread_adj_nmv.loc['Total'] = [spread_adj_nmv['Spread Adj NMV'].sum(), None, None]

    return unadj_nmv, spread_adj_nmv


def highlight_total(top_left):
    ttl_cell = top_left.expand('down')[-1]
    ttl_cell.font.bold = True
    ttl_cell.expand('right')[1].color = (255, 255, 0)


if __name__ == '__main__':
    try:
        conn = return_pyranha_conn()
        pos_df = get_pos_df(conn)
        security_df = get_security_df(conn, '2023-03-18')
        conn.stop()

        with pd.ExcelWriter('last_ddl.xlsx') as writer:
            pos_df.drop('Timestamp', axis=1).to_excel(writer, 'pos_df')
            security_df.to_excel(writer, 'security_df')
    except ConnectionError:
        pos_df, security_df = pd.read_excel('last_ddl.xlsx').values()

    hedge_factors = pd.read_excel(fname, index_col=0, skiprows=3)
    hedge_factors.index = hedge_factors.index.date

    '''Compute equity stats'''
    equity_cols = [0, 1, 2, 3]
    equity_fin_types = ['ETF', 'COMMON', 'OPTION', 'FUTURES']

    hist_x = hedge_factors.iloc[:, equity_cols]
    X = hist_x.pct_change()

    nmv_equity = agg_nmv(pos_df[pos_df['FinancialType'].isin(equity_fin_types)].set_index('USym')['NMV'])
    hist_y = nmv_equity.reset_index()['USym'].apply(lambda x: get_factor_values(x, security_df)).T
    hist_y.columns = nmv_equity.index

    x_labels = ['NDX Index', 'SPX Index']
    y_labels = ['NQM24', 'ESM24']
    hist_y = hist_x[x_labels].join(hist_y)
    hist_y[y_labels] = hist_y[x_labels]
    hist_y.drop(x_labels, axis=1, inplace=True)
    hist_y.sort_index(inplace=True)
    for label in y_labels:
        nmv_equity.loc[label] = 0
    Y = hist_y.apply(lambda x: x.dropna().pct_change())

    periods = {'1M': 22, '3M': 66, '6M': 132, '12M': 252}
    corrs = {period: compute_all_x_and_y(X, Y, days, compute_corr) for period, days in periods.items()}
    betas = {period: compute_all_x_and_y(X, Y, days, compute_beta) for period, days in periods.items()}
    betas_adj = {period: compute_beta_adj_nmv(beta, nmv_equity.values) for period, beta in betas.items()}
    equity_results = {'Correls': corrs, 'Betas': betas, 'Betas_Adj_NMV': betas_adj}

    '''Compute credit stats'''
    spread_cols = [5, 6, 7, 8]
    credit_fin_types = ['CORP BOND', 'CREDIT DEFAULT']
    credit_cols = ['BbgDesc', 'SettleCurrency', 'RatingsBucket', 'NMV', 'YAS_YLD_SPREAD', 'CDS_FLAT_SPREAD']

    spread_denoms = dict(zip(['USD_HY', 'USD_IG', 'EUR_HY', 'EUR_IG'], hedge_factors.iloc[-1, spread_cols]))
    credit_info = pos_df.loc[pos_df['FinancialType'].isin(credit_fin_types), credit_cols].set_index('BbgDesc')
    credit_info.loc[credit_info['SettleCurrency'] == 'GBP', 'SettleCurrency'] = 'EUR'
    missing = credit_info[~credit_info['RatingsBucket'].isin(['HY', 'IG'])].index  # Unrated or None
    credit_info.loc[missing, 'RatingsBucket'] = missing.map(lambda x: 'IG' if 'CDX IG' in x else 'HY')
    credit_info['Class'] = credit_info['SettleCurrency'] + '_' + credit_info['RatingsBucket']
    credit_info['YLD/CDS Spread'] = credit_info['YAS_YLD_SPREAD'].fillna(credit_info['CDS_FLAT_SPREAD'])
    credit_results = {cl: compute_spread_adj_nmv(df, spread_denoms[cl]) for cl, df in credit_info.groupby('Class')}

    '''Write to Excel'''
    wb = xwfn.new_book(output_fname)

    # write equity results
    for sheet, dfs in equity_results.items():
        ws = xwfn.new_sheet('Equity_' + sheet, wb)
        col = 0
        if sheet == 'Betas_Adj_NMV':
            xwfn.df_to_excel(nmv_equity, ws[1, col], '#,##0_ ;[Red]-#,##0')
            col += 3

        for period, df in dfs.items():
            ws[0, col].value = period
            xwfn.df_to_excel(df, ws[1, col], '#,##0.00_ ;[Red]-#,##0.00')

            if sheet == 'Betas_Adj_NMV':
                highlight_total(ws[1, col])

            col += X.shape[1] + 2

        ws.autofit()

    # write credit results
    ws = xwfn.new_sheet('Credit', wb)
    row = 0
    for curr in ('USD', 'EUR'):
        ws[row, 0].value = curr
        ws[row, 0].font.bold = True

        col = 0
        for cl in ('HY', 'IG'):
            ws[row + 1, col].value = cl
            ws[row + 1, col].font.italic = True

            unadj_nmv, spread_adj_nmv = credit_results[curr + '_' + cl]
            xwfn.df_to_excel(unadj_nmv, ws[row + 2, col], '#,##0_ ;[Red]-#,##0')
            xwfn.df_to_excel(spread_adj_nmv, ws[row + 2, col + 3], '#,##0.00_ ;[Red]-#,##0.00')
            highlight_total(ws[row + 2, col + 3])
            col += 8

        row += max(credit_results[curr + '_' + k][0].shape[0] for k in ('HY', 'IG')) + 8

    ws.autofit()

    xwfn.autofit_workbook(wb)
    wb.save()
