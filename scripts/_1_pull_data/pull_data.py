from _1_pull_data.subroutines import basic_data, channel_main, channel_ts, pervideo_main, pervideo_ts


def pull_data():
    basic_data.pull_basic_data()

    channel_main.pull_channel_main()

    channel_ts.pull_channel_ts()

    pervideo_main.pull_pervideo_main()

    pervideo_ts.pull_pervideo_ts()
