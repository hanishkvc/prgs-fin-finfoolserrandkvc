# Plot data
# HanishKVC, 2021
# GPL

import matplotlib.pyplot as plt


def data(dataSrcs, entCodes, startDate=-1, endDate=-1):
    """
    Plot specified datas for the specified MFs, over the specified date range.

    dataSrcs: Is a key or a list of keys used to retreive the data from gEnts.data.
    entCodes: Is a entCode or a list of entCodes.
    startDate and endDate: specify the date range over which the data should be
        retreived and plotted.

    Remember to call plt.plot or show_plot, when you want to see the plots,
    accumulated till that time.
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    if type(dataSrcs) == str:
        dataSrcs = [ dataSrcs ]
    if type(entCodes) == int:
        entCodes = [ entCodes]
    srelMetaData, srelMetaLabel = data_metakeys('srel')
    for dataSrc in dataSrcs:
        print("DBUG:plot_data:{}".format(dataSrc))
        dataSrcMetaData, dataSrcMetaLabel = data_metakeys(dataSrc)
        for entCode in entCodes:
            index = gEnts.meta['codeD'][entCode]
            name = gEnts.meta['name'][index][:giLabelNameChopLen]
            try:
                dataLabel = gEnts.data[dataSrcMetaLabel][index]
            except:
                dataLabel = ""
            label = "{}:{:{width}}: {}".format(entCode, name, dataLabel, width=giLabelNameChopLen)
            print("\t{}:{}".format(label, index))
            label = "{}:{:{width}}: {:16} : {}".format(entCode, name, dataSrc, dataLabel, width=giLabelNameChopLen)
            plt.plot(gEnts.data[dataSrc][index, startDateIndex:endDateIndex+1], label=label)


def _show():
    """
    Show the data plotted till now.
    """
    leg = plt.legend()
    plt.setp(leg.texts, family='monospace')
    for line in leg.get_lines():
        line.set_linewidth(8)
    plt.grid(True)
    startDateIndex, endDateIndex = _date2index(-1,-1)
    curDates = gEnts.dates[startDateIndex:endDateIndex+1]
    numX = len(curDates)
    xTicks = (numpy.linspace(0,1,9)*numX).astype(int)
    xTicks[-1] -= 1
    xTickLabels = numpy.array(curDates)[xTicks]
    plt.xticks(xTicks, xTickLabels, rotation='vertical')
    plt.show()


def show():
    """
    Show the data plotted till now.
    """
    _show_plot()


