from _2_manage_sheets.subroutines_new import change_name_and_freeze_justnew, create_sheets_for_new_videos, move_to_correct_folder_justnew


def manage_sheets():
    new_videos = create_sheets_for_new_videos.create_sheets()

    if new_videos > 0:
        change_name_and_freeze_justnew.change_name_and_freeze(new_videos)
        move_to_correct_folder_justnew.move_to_correct_folder(new_videos)
    else:
        print('No new videos')
