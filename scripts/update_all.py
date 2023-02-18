from _1_pull_data import pull_data
from _2_manage_sheets import manage_sheets
from _3_write_to_db import create_db
from _4_assemble_data import assemble_data
from _5_upload import upload_all

pull_data.pull_data()

manage_sheets.manage_sheets()

create_db.create_db()

assemble_data.assemble_data()

upload_all.upload_view_only()

upload_all.upload_video_data()