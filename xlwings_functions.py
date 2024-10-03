import pandas as pd
import xlwings as xw


def autofit_workbook(wb):
    """
    Autofit entire workbook.

    :param wb: Excel workbook.
    :return:
    """
    for ws in wb.sheets:
        max_row, max_col = pd.read_excel(wb.name, ws.name, header=None)
        ws.autofit()
        for j in range(max_col):
            ws[0, j].column_width = max(8.09, ws[0, j].column_width)


def new_book(name):
    try:
        return xw.Book(name)
    except FileNotFoundError:
        xw.App()
        wb = xw.books.add()
        wb.save(name)
        return wb


def new_sheet(name, wb):
    """
    Clear the current version of the sheet if it already exists else add a new sheet.

    :param name: Excel sheet name.
    :param wb: Excel workbook.
    :return ws: Excel worksheet.
    """
    print('Creating {} sheet'.format(name))
    if name in wb.sheet_names:
        ws = wb.sheets[name]
        ws.clear()
        for chart in ws.charts:
            chart.delete()
        return ws
    else:
        return wb.sheets.add(name, after=wb.sheets[-1])


def xlwings_plot(ws, y_values, x_values, top_left, bottom_right, chart_type='line', title=None, legend=False,
                 x_label=None, y_label=None, x_number_format='General', y_number_format='General'):
    def get_left_and_top(cell_loc):
        cell = ws[cell_loc]
        cell_range = ws.range((1, 1), (cell.row, cell.column))
        return cell_range.width, cell_range.height

    left, top = get_left_and_top(top_left)
    right, bottom = get_left_and_top(bottom_right)
    width = right - left
    height = top - bottom
    chart = ws.charts.add(left, top, width, height)
    chart.chart_type = chart_type
    # chart.set_source_data(cells)
    wrapper = chart.api[1]
    wrapper.HasLegend = legend
    series = wrapper.SeriesCollection().NewSeries()
    series.Values = y_values
    series.XValues = x_values

    if title is not None:
        wrapper.SetElement(2)
        wrapper.ChartTitle.Text = title

    x_axis, y_axis = (wrapper.Axes(i) for i in (1, 2))
    x_axis.TickLabels.NumberFormat = x_number_format
    y_axis.TickLabels.NumberFormat = y_number_format
    x_axis.TickLabelPosition = -4134

    if x_label is not None:
        x_axis.HasTitle = True
        x_axis.AxisTitle.Text = x_label

    if y_label is not None:
        y_axis.HasTitle = True
        y_axis.AxisTitle.Text = y_label



def df_to_excel(df, top_left, number_format):
    top_left.value = df.fillna(0)
    cells = top_left.expand('table')
    cells[1:, 0].api.Borders.Weight = 2
    cells[0, 1:].api.Borders.Weight = 2
    cells[1:, 1:].number_format = number_format
    top_left.value = df
    top_left.value = None
