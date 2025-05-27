"""
Clickup NALK module for data extraction functions.
This module contains functions specific to the Clickup NALK integration.
"""

from core import gcs


def run(customer):
    import requests
    import os
    import pathlib
    from google.cloud import storage

    API_BEARER_TOKEN = customer['api_bearer_token']
    API_PAYLOAD = customer['api_payload']
    BUCKET_NAME = customer['bucket_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    credentials = gcs.load_credentials_from_env()
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)

    url = "https://cu-prod-prod-us-west-2-2-export-service.clickup.com/v1/exportView"
    headers = {
        "Authorization": f"Bearer {API_BEARER_TOKEN}",
        "Content-Type": "application/json",
        "x-csrf": "1",
        "x-workspace-id": "9011119715"
    }

    params = {
        "date_type": "normal",
        "time_format": "normal",
        "name_only": "False",
        "all_columns": "True",
        "async": "True",
        "time_in_status_total": "False"
    }

    # Substitua o payload abaixo pelo seu JSON (como o que você colou)
    payload = {"id": "8chnhk3-24151", "members": [], "group_members": [], "name": "Clientes | Tarefas",
               "parent": {"id": "901103907816", "type": 6}, "type": 1, "creator": 75379731, "pinned": True,
               "me_view": False, "locked": False, "visibility": 1,
               "settings": {"show_task_locations": False, "show_subtasks": 1, "show_subtask_parent_names": False,
                            "show_closed_subtasks": False, "show_assignees": True, "show_images": True,
                            "show_timer": False,
                            "collapse_empty_columns": False, "me_comments": True, "me_subtasks": True,
                            "me_checklists": True, "show_empty_statuses": None, "auto_wrap": False,
                            "time_in_status_view": 1, "is_description_pinned": False,
                            "override_parent_hierarchy_filter": False, "fast_load_mode": False,
                            "show_task_properties": True,
                            "show_sprint_cards": {"show_add_tasks": True, "show_add_estimates": True,
                                                  "show_add_assignees": True}, "show_empty_fields": False,
                            "field_rendering": 1, "colored_columns": True, "card_size": 2, "task_cover": 2},
               "embed_settings": None, "grouping": {"field": "cf_815b78bd-b5a5-4bd8-88a7-aa803dec5e86", "dir": 1,
                                                    "collapsed": ["901101648673_previsão", "901103907816_onboarding",
                                                                  "901103907816_ongoing", "901103907816_churn",
                                                                  "901103907816_contrato assinado",
                                                                  "901103907816_previsão",
                                                                  "901103907816_em andamento", "901103907816_cliente",
                                                                  "df50c7ca-f780-4295-84f4-7f6460b440f4"],
                                                    "ignore": False,
                                                    "single": False},
               "divide": {"field": None, "dir": None, "collapsed": [], "by_subcategory": None},
               "sorting": {"fields": [{"field": "assignee", "dir": 1}]},
               "frozen_by": {"id": 0, "username": None, "email": None, "color": None, "initials": None,
                             "profilePicture": None},
               "filters": {"search": "", "show_closed": False, "search_custom_fields": True, "search_description": True,
                           "search_name": True, "op": "AND", "filter_group_ops": [], "filter_groups": [], "fields": []},
               "columns": {"fields": [{"field": "startDate", "width": 160, "hidden": False, "name": None},
                                      {"field": "dueDate", "width": 160, "hidden": False, "name": None},
                                      {"field": "commentCount", "width": 80, "hidden": False, "name": None},
                                      {"field": "assignee", "width": 80, "hidden": False, "name": None},
                                      {"field": "cf_ef0841d7-0ef8-40d2-a89c-0215f14a687f", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_59a6f721-2f68-4afb-b78b-6632d2e34c1a", "width": 178, "hidden": True,
                                       "name": None},
                                      {"field": "cf_8cae81d2-b11c-431d-b786-6f373720bab6", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_33068e4b-e7b3-4cbf-84f4-d2d76a8a1f94", "width": 132,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_3929cc7a-4de3-44c2-8ae2-ff61fc594e96", "width": 212, "hidden": True,
                                       "name": None},
                                      {"field": "cf_32afd909-21e2-4c0d-987b-b60568eb6cf6", "width": 208, "hidden": True,
                                       "name": None},
                                      {"field": "cf_79a247f8-f86d-4425-a2b9-dbb261b35959", "width": 117,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_d963bafa-b0b3-42a5-80b3-f424eebe6550", "width": 160,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_804cba43-a892-4bc0-9b61-c653cf387d1f", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "priority", "width": 112, "hidden": False, "name": None},
                                      {"field": "cf_a0979ca5-37b0-4a9e-ba5b-4bbc7ee253a0", "width": 105,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_41417090-aea0-44d1-9b8f-12027bbb1b03", "width": 110,
                                       "hidden": False,
                                       "name": None, "calculation": {"func": "sum", "unit": "", "groups": []}},
                                      {"field": "cf_d94d0f2e-2dbe-4186-8b53-8dd0026472fd", "width": 131, "hidden": True,
                                       "name": None, "calculation": {"func": "sum", "unit": "", "groups": []}},
                                      {"field": "cf_003b874a-30f8-4143-aee7-aafe983f0f52", "width": 168,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_4df2d003-3487-4215-8706-3e3c59bac58d", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_bf3e8994-e614-4c32-a9f1-e8c385064e5b", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_ca57a296-5780-44e0-a5c8-7a9f68e0d3ef", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_32cc68cd-2473-4f6b-b0c9-b88104c098f0", "width": 160, "hidden": True,
                                       "name": None}, {"field": "status", "width": 201, "hidden": True, "name": None},
                                      {"field": "name", "width": 448, "hidden": True, "name": None},
                                      {"field": "id", "width": 160, "hidden": True, "name": None},
                                      {"field": "customId", "width": 160, "hidden": True, "name": None},
                                      {"field": "dateCreated", "width": 160, "hidden": True, "name": None},
                                      {"field": "dateDone", "width": 160, "hidden": True, "name": None},
                                      {"field": "dateUpdated", "width": 160, "hidden": True, "name": None},
                                      {"field": "duration", "width": 160, "hidden": True, "name": None},
                                      {"field": "timeLoggedRollup", "width": 160, "hidden": True, "name": None},
                                      {"field": "timeEstimateRollup", "width": 160, "hidden": True, "name": None},
                                      {"field": "sprints", "width": 185, "hidden": True, "name": None},
                                      {"field": "pointsEstimateRollup", "width": 160, "hidden": True, "name": None},
                                      {"field": "dateClosed", "width": 160, "hidden": True, "name": None},
                                      {"field": "createdBy", "width": 160, "hidden": True, "name": None},
                                      {"field": "latestComment", "width": 160, "hidden": True, "name": None},
                                      {"field": "lists", "width": 185, "hidden": True, "name": None},
                                      {"field": "pullRequests", "width": 160, "hidden": True, "name": None},
                                      {"field": "incompleteCommentCount", "width": 160, "hidden": True, "name": None},
                                      {"field": "timeInStatus", "width": 160, "hidden": True, "name": None},
                                      {"field": "linked", "width": 160, "hidden": True, "name": None},
                                      {"field": "dependencies", "width": 160, "hidden": True, "name": None},
                                      {"field": "pages", "width": 160, "hidden": True, "name": None},
                                      {"field": "cf_732c2bc5-dc9f-4ee9-bd00-b921023ce8ab", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_892f7ea2-c462-4bd2-a41d-7db371756b5c", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_3f84df19-e89c-4ba8-b5da-4352bc40a843", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_f18b988a-e03e-4f97-995c-67f749304df3", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_b150e268-0db6-4be0-96e7-259c9cedbc45", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_69e83cbc-0aa7-4702-83fa-268a3c59c560", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_1870d0af-4c2f-42e3-ad4b-320417557a1c", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_4f0a03bd-58bc-42a8-acc0-73c80d8e6e79", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_f7a4da81-c260-461b-b92b-3929ca8e3ef1", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_6bf7aa9c-1bac-4d43-9d69-924ef56b89de", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_9361c5bd-1f6a-4e96-b049-179545c7abb6", "width": 400,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_33d07b7a-e625-44cf-9367-47a74e6912a5", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_cfac36e3-5465-46c6-b863-95370ba2c70c", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_e4ff4ff3-cc01-480b-b24a-c9fe8a0d8f79", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_0b0dad5e-5a08-4cd7-a0b2-5d0d6183f493", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_b5e01bc1-7268-4af8-97dd-89ecb74079c4", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_8a1d5e98-f924-40a3-85cf-a02aaca0a7b0", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_f987f3a8-8efe-4afe-ae2d-a11ac23dd2ab", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_10c44588-7722-4816-9c04-64e4af62d473", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_6398f1a2-fd8b-4dfb-bdf7-b484f03bd3d6", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_e70f9830-9022-46b1-9a9a-35ee97472aba", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_d7d1172a-7372-44c9-b87e-76d4978270fd", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_ae0cb522-fa4f-4e22-b5aa-41cca266f30e", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_6dd3ff8b-240b-4f92-aa5c-6e7fa34841f2", "width": 160,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_efe4d04f-bbbd-4cc9-b259-b00ff14ebac7", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_7dbe97d7-7e75-4ef1-9a36-e0a4ed424a5a", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_879ebb2e-0f04-4c07-b048-ea6c5205d2fb", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_d331dbc2-a72b-4b95-8e6a-a9d82d3cde46", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_e2ca91fb-b21d-469c-a068-7ed5abc712d7", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_c9c0ac03-8ec0-431a-83d4-e799d0d3f0c0", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_e7bf1f4b-2253-4d68-9980-483b60d41e01", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_1c05776c-8476-41cc-8d05-78b2a663807d", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_8ec841ca-3dde-4cb0-adaf-cd2559909a88", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_b2376c70-7c85-4db9-9ffe-8b7e289f3aac", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_8d277efd-5605-484e-8238-88f441449077", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_9f32c049-e23e-4b1a-8a00-f911e0b2e4c5", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_a1aa4cf1-262b-42d6-beca-6677cb8a2989", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_c393d721-92e7-4f0d-8787-62dd766379e6", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_4611ba63-cb81-4a8e-a382-f482679ad275", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_815b78bd-b5a5-4bd8-88a7-aa803dec5e86", "width": 160,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_5f507b01-294b-45de-b8f4-c9e27d7bcea3", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_be1a1658-5cd0-418f-ba30-39da6e8b1d86", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_cf97d025-e034-4dad-a0ea-f8b256c1b9d8", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_d9da3852-649e-45cc-a685-8303cfaa7b21", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_52a4481d-36ee-4960-ab87-57dfe0a442ee", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_bca61a87-4e7f-4c99-819c-338180d6e6dc", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_ef126f5e-e1a4-468d-b1ae-d7196d6a0137", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_3d4bb33d-dbf7-4391-95c9-d10cef4e65da", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_e3547e9e-b93c-45ec-ac8b-814e6f44e003", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_c6f2b3f0-8957-41e7-86a7-9eaf290cca48", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_7ada6248-ef59-477a-8d4a-1ba726fdfc52", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_2f524089-744f-477c-a1b6-c0ab98d73c0c", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_f3ad02e1-b1f8-45a9-93ec-103365444a47", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_a07e5b53-060d-44dd-a549-6a7b8942090f", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_d045f36a-5cd7-49b1-8d6c-8c6ef3d7fc0f", "width": 293,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_89f6f730-7f0d-4f03-9abb-6ea506ee4009", "width": 160,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_b7e2c715-1a09-45ed-b40d-150000e38e90", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_b7e987ea-77f4-44f7-948b-a7090951eb08", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_20ef6274-3db9-49ba-96e0-059543adc52e", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_2a356f34-49fe-45e3-98d3-5aca9fdcbd50", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_83365d4f-0fea-49fb-8492-c97f0873c6e8", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_811ea4a2-3092-4a76-a6eb-c22986482dc7", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_41a8122e-3c4d-4422-9f50-7ebd93fc6a0c", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_319920aa-08a4-4bf8-819a-089ff2985b49", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_673dc69f-dfd5-4a56-baf1-d03fffe653c0", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_7ee4810b-1720-4f1b-85f4-8443d26f72ee", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_b06533af-5045-4cc0-b9a2-2a40d3d2017d", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_2805f423-8aad-4131-a9f4-ec53d01feb99", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_24515b38-b0ce-4c1e-9364-0b196acff719", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_912cffe6-a6dc-4248-a59a-9636e3fef416", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_2fcdf1d3-b767-4060-8919-d06a33c25f8d", "width": 160,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_9e6e3cd4-de2e-49d3-9105-adb10ceb1dab", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_be8eb95f-60f8-473b-81f8-387ee9feb188", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_8a9b8825-2d8c-4a1c-b618-c57a9445c70f", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_824b55be-ef7b-4029-8827-453e4afb29c5", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_0fecdb9d-6ca2-4cda-8ef9-90866a317230", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_1c988a55-1887-4d9a-abe0-f608a9be6cc4", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_1d1ffb6f-b0eb-49ce-951a-b2cb5ae70e05", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_10e623f7-45f1-46c1-af77-3f1c2d5b12b1", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_086041cd-e6bb-4c32-964a-8a9346de129d", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_8a5fdb99-2055-459a-b393-8426dd4e6234", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_99daeeaf-53bf-4f44-9675-c525d5525874", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_4ae9fcea-d501-41e9-8098-e49522064f2a", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_2e6888c6-c046-4c6c-bbdb-0279ed143b8b", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_1586add1-6de7-4e2f-ba91-5429413d4d46", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_05280a98-644a-445a-9d26-1d1ae498d1bb", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_4ee210b3-e9dc-4dc8-ae85-8484a096bc15", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_3d0b3c95-6ca1-4cd6-a651-cabe15ee3648", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_4bebc255-79ce-4ac8-98df-9049094b058b", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_35ef67cd-9706-4fee-b972-608cd265d651", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_ef79bf22-f43a-4597-ac94-9b35aa8a5b97", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_143fa92f-ac34-4fa8-bb61-55a89a334c85", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_65d9bd49-f9fa-4597-a439-1fa66852247e", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_d843e330-79c6-408b-9d8a-cf9b9acfcc36", "width": 160,
                                       "hidden": False,
                                       "name": None},
                                      {"field": "cf_53b5c897-f2ca-481a-8df9-9d230b5cac80", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_3c716633-9625-4f2c-9ae2-88d56a73b941", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_9276c2ef-44cd-4350-9ec9-99984f4b8ffa", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_80ffd273-7782-4731-abd4-79afebe06449", "width": 160, "hidden": True,
                                       "name": None},
                                      {"field": "cf_6b72e8a1-b173-4f36-8f95-43c97a8d18b0", "width": 160, "hidden": True,
                                       "name": None}]}, "default": False, "standard": True, "standard_view": True,
               "orderindex": 10, "public": False, "seo_optimized": False, "public_duplication_enabled": False,
               "shared_with_me": False, "tasks_shared_with_me": False,
               "team_sidebar": {"assigned_comments": False, "assignees": [], "group_assignees": [],
                                "unassigned_tasks": False}, "frozen_note": None, "public_share_expires_on": None,
               "share_tasks": None,
               "share_task_fields": ["assignees", "priority", "due_date", "content", "comments", "attachments",
                                     "customFields", "subtasks", "tags", "checklists", "coverimage"],
               "board_settings": {},
               "team_id": "9011119715", "sidebar_view": False, "sidebar_orderindex": None,
               "sidebar_num_subcats_between": 0,
               "doc_type": 1, "unfolded_st_ids": []}

    # Função para fazer upload direto para o GCS
    def upload_csv_to_gcs(csv_content, destination_path):
        try:
            blob = bucket.blob(destination_path)
            blob.upload_from_string(csv_content, content_type="text/csv")
            print(f"✅ CSV salvo em gs://{BUCKET_NAME}/{destination_path}")
            return True
        except Exception as e:
            print(f"❌ Falha ao fazer upload para {destination_path}: {str(e)}")
            return False

    print("[INFO] Iniciando requisição para exportar dados do ClickUp...")

    response = requests.post(url, headers=headers, params=params, json=payload)
    if response.ok:
        data = response.json()
        csv_url = data.get("url")  # pega o campo 'url'

        if csv_url:
            print("📎 Link do CSV obtido:", csv_url)

            # Passo 3: faz o download do CSV
            print("[INFO] Fazendo download do CSV...")
            csv_response = requests.get(csv_url)

            if csv_response.ok:
                # Salva diretamente no Google Cloud Storage
                destination_path = "clickup/clientes.csv"

                if upload_csv_to_gcs(csv_response.content.decode('utf-8'), destination_path):
                    print(f"✅ Processo finalizado! CSV salvo no GCS em: gs://{BUCKET_NAME}/{destination_path}")
                else:
                    print("❌ Falha ao salvar no GCS")
            else:
                print(f"❌ Falha ao baixar o CSV: {csv_response.status_code}")
                print(csv_response.text)
        else:
            print("❌ Campo 'url' não encontrado na resposta.")
            print("Resposta completa:", data)
    else:
        print(f"❌ Erro ao fazer POST: {response.status_code}")
        print("Resposta:", response.text)


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for ClickUp NALK.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
