from paths_config import USER, PASSWORD, PATH_TO_DB, PATH_TO_MESSAGES_MEDIA, PATH_TO_USER_PICTURES
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
from sqlalchemy import select, and_, insert, update

def authorization(db, json_request):
    j_user_id = json_request["user_id"]
    j_token = json_request["token"]
    response = {"status": False,
                "db_error": False}
    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = select([user_sessions])
                stmt = stmt.where(and_(user_sessions.c.user_id == j_user_id, user_sessions.c.active == 1,
                                       user_sessions.c.token == j_token))
                result = conn.execute(stmt).fetchall()
        if len(result) == 1:
            response["status"] = True
    except:
        response["db_error"] = True
    return response


def auth_response(json_request, json_response):
    if "user_id" in json_request and "token" in json_request:
        auth_response_dict = authorization(json_request)
        if not auth_response_dict["status"]:
            json_response["status"] = False
            json_response["user_id"] = False
            json_response["token"] = False
            if auth_response_dict["db_error"]:
                json_response["db_error"] = True
    else:
        json_response["status"] = False
        json_response["user_id"] = None
        json_response["token"] = None

    return json_response


def get_media(path_to_media):
    if path_to_media is not None:
        with open(path_to_media, "rb") as file:
            media = base64.b64encode(file.read()).decode()
        return media
    else:
        return None


def post_media(path_to_dir, entity_id, media):

    entity_dir = path_to_dir + "/" + str(entity_id)

    if not os.path.exists(entity_dir):
        os.mkdir(entity_dir)

    multimedia_file = base64.b64decode(media)
    file_name = uuid.uuid4().hex
    multimedia_path = entity_dir + '/' + file_name
    with open(multimedia_path, "wb") as file:
        file.write(multimedia_file)

    return multimedia_path