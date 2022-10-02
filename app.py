from flask import Flask, request
from flask_cors import CORS
from paths_config import USER, PASSWORD, PATH_TO_DB, PATH_TO_MESSAGES_MEDIA, PATH_TO_USER_PICTURES
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.sql import func
from sqlalchemy import select, and_, insert, update
import hashlib
import os
import base64
import json
import uuid

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+pymysql://{USER}:{PASSWORD}@{PATH_TO_DB}'
db = SQLAlchemy(app)
db.create_all()
CORS(app)

users = db.Table("users", db.metadata, autoload=True, autoload_with=db.engine)
messages = db.Table("messages", db.metadata, autoload=True, autoload_with=db.engine)
multimedia = db.Table("multimedia", db.metadata, autoload=True, autoload_with=db.engine)
user_sessions = db.Table("user_sessions", db.metadata, autoload=True, autoload_with=db.engine)
user_pictures = db.Table("user_pictures", db.metadata, autoload=True, autoload_with=db.engine)
chats = db.Table("chats", db.metadata, autoload=True, autoload_with=db.engine)
chat_users = db.Table("chat_users", db.metadata, autoload=True, autoload_with=db.engine)


def authorization(json_request):
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


@app.route('/signup/', methods=["POST"])
def sign_up():
    request_data = request.get_json()

    r_login = None
    r_password = None
    r_username = None
    r_nickname = None
    r_bio = None

    json_response = {"status": True,
                     "db_error": False,
                     "login": True,
                     "password": True,
                     "username": True
                     }

    if "login" in request_data:
        r_login = request_data["login"]
    if "password" in request_data:
        r_password = request_data["password"]
    if "username" in request_data:
        r_username = request_data["username"]
    if "nickname" in request_data:
        r_nickname = request_data["nickname"]
    if "bio" in request_data:
        r_bio = request_data["bio"]


    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = insert(users).values(
                    login=r_login,
                    password=hashlib.md5(r_password.encode()).hexdigest(),
                    username=r_username,
                    nickname=r_nickname,
                    bio=r_bio)
                conn.execute(stmt)
    except:
        json_response["status"] = False
        json_response["db_error"] = True

    return json.dumps(json_response, default=str)


@app.route('/login/', methods=["POST"])
def login():
    request_data = request.get_json()
    json_response = {"status": True,
                     "db_error": False,
                     "login": True,
                     "password": True,
                     "user_id": None,
                     "token": None
                     }

    r_login = None
    r_password = None

    if "login" in request_data:
        r_login = request_data["login"]
    if "password" in request_data:
        r_password = request_data["password"]
    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = select([users.c.password])
                stmt = stmt.where(users.c.login == r_login)
                result = conn.execute(stmt).fetchall()
    except:
        json_response["status"] = False
        json_response["db_error"] = True
        return json.dumps(json_response, default=str)

    if len(result) == 0:
        json_response["status"] = False
        json_response["login"] = False
        json_response["password"] = False
    else:
        if hashlib.md5(r_password.encode()).hexdigest() != dict(result[0])["password"]:
            json_response["status"] = False
            json_response["password"] = False
            return json.dumps(json_response, default=str)
        else:
            token = str(uuid.uuid4())
            ip_address = request.environ.get('HTTP_X_FORWARDED_FOR', request.remote_addr)
            device = request.user_agent.platform
            try:
                with db.engine.connect().execution_options(autocommit=True) as conn:
                    with conn.begin():
                        stmt = select([users.c.user_id])
                        stmt = stmt.where(users.c.login == r_login)
                        result = conn.execute(stmt).fetchall()
            except:
                json_response["status"] = False
                json_response["db_error"] = True
                return json.dumps(json_response, default=str)

            r_user_id = dict(result[0])["user_id"]
            try:
                with db.engine.connect().execution_options(autocommit=True) as conn:
                    with conn.begin():
                        stmt = insert(user_sessions).values(
                            user_id=r_user_id,
                            token=token,
                            active=1,
                            ip=ip_address,
                            device=device)
                        conn.execute(stmt)
            except:
                json_response["status"] = False
                json_response["db_error"] = True
                return json.dumps(json_response, default=str)

            json_response["token"] = token
            json_response["user_id"] = r_user_id
    return json.dumps(json_response, default=str)


@app.route('/chats/', methods=["POST"])
def get_chats():
    request_data = request.get_json()

    json_response = {"status": True,
                     "db_error": False,
                     "user_id": True,
                     "token": True
                     }

    json_response = auth_response(request_data, json_response)
    if json_response["status"]:
        r_user_id = request_data["user_id"]
    else:
        return json.dumps(json_response, default=str)

    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                up_1 = user_pictures.alias("up_1")
                up_2 = user_pictures.alias("up_2")

                mes_1 = messages.alias("mes_1")
                mes_2 = messages.alias("mes_2")

                subq_mes_id = select([func.max(mes_2.c.message_id)])
                subq_mes_id = subq_mes_id.where(mes_2.c.chat_id == chat_users.c.chat_id).scalar_subquery()

                subq_chat_id = select([chat_users.c.chat_id])
                subq_chat_id = subq_chat_id.where(chat_users.c.user_id == r_user_id).scalar_subquery()

                subq_last_pic = select([up_2.c.picture])
                subq_last_pic = subq_last_pic.where(up_2.c.user_id == chat_users.c.user_id)
                subq_last_pic = subq_last_pic.order_by(up_2.c.loaded.desc())
                subq_last_pic = subq_last_pic.limit(1).scalar_subquery()

                stmt = select([chat_users.c.chat_id, chat_users.c.user_id,
                              users.c.nickname, subq_last_pic.label("picture"),
                              mes_1.c.message_id, mes_1.c.message_text,
                              func.count(multimedia.c.multimedia).label("multimedia_amount"),
                              mes_1.c.sender_id, mes_1.c.sending_time,
                              mes_1.c.checked])
                stmt = stmt.select_from(chat_users)
                stmt = stmt.join(users, chat_users.c.user_id == users.c.user_id, isouter=True)
                stmt = stmt.join(mes_1, mes_1.c.chat_id == chat_users.c.chat_id, isouter=True)
                stmt = stmt.join(multimedia, multimedia.c.message_id == mes_1.c.message_id, isouter=True)
                stmt = stmt.where(and_(chat_users.c.chat_id.in_(subq_chat_id),
                                      chat_users.c.user_id != r_user_id))
                stmt = stmt.group_by(mes_1.c.message_id)
                stmt = stmt.having(mes_1.c.message_id.in_(subq_mes_id))
                stmt = stmt.order_by(mes_1.c.message_id.desc())
                result = conn.execute(stmt).fetchall()


    except:
        json_response["status"] = False
        json_response["db_error"] = True
        json_response["user_id"] = None
        json_response["token"] = None
        return json.dumps(json_response, default=str)

    json_response["response"] = list()
    for row in result:
        chats_dict = dict(row)
        chats_dict["picture"] = get_media(chats_dict["picture"])
        json_response["response"].append(chats_dict)

    return json.dumps(json_response, default=str)


@app.route('/message/', methods=["POST"])
def post_message():
    request_data = request.get_json()
    json_response = {"status": True,
                     "db_error": False,
                     "user_id": None,
                     "token": None
                     }

    r_user_id = None
    r_chat_id = None
    r_receiver_id = None
    r_message_text = None
    r_multimedia = None

    json_response = auth_response(request_data, json_response)
    if json_response["status"]:
        r_user_id = request_data["user_id"]
    else:
        return json.dumps(json_response, default=str)

    if "chat_id" in request_data:
        r_chat_id = request_data["chat_id"]

    elif "receiver_id" in request_data:
        r_receiver_id = request_data["receiver_id"]
        try:
            with db.engine.connect() as conn:
                cu_sender = chat_users.alias("cu_sender")
                cu_receiver = chat_users.alias("cu_receiver")
                stmt = select([cu_sender.c.chat_id.distinct()])
                stmt = stmt.select_from(cu_sender.join(cu_receiver, cu_receiver.c.chat_id == cu_sender.c.chat_id))
                stmt = stmt.where(and_(cu_receiver.c.user_id == r_receiver_id, cu_sender.c.user_id == r_user_id))
                result = conn.execute(stmt).fetchall()
        except:
            json_response["status"] = False
            json_response["db_error"] = True
            return json.dumps(json_response, default=str)
        if len(result) == 1:
            r_chat_id = dict(result[0])["chat_id"]
        elif len(result) == 0:
            try:
                with db.engine.connect().execution_options(autocommit=True) as conn:
                    with conn.begin():
                        stmt = insert(chats).values()
                        result = conn.execute(stmt)
            except:
                json_response["status"] = False
                json_response["db_error"] = True
                return json.dumps(json_response, default=str)
            ins_chat_id = result.inserted_primary_key[0]
            try:
                with db.engine.connect() as conn:
                    stmt_sender = insert(chat_users).values(
                        chat_id=ins_chat_id,
                        user_id=r_user_id
                    )
                    stmt_receiver = insert(chat_users).values(
                        chat_id=ins_chat_id,
                        user_id=r_receiver_id
                    )
                    conn.execute(stmt_sender)
                    conn.execute(stmt_receiver)
            except:
                json_response["status"] = False
                json_response["db_error"] = True
                return json.dumps(json_response, default=str)
            r_chat_id = ins_chat_id
    json_response["chat_id"] = r_chat_id

    if "message_text" in request_data:
        r_message_text = request_data["message_text"]
    if "multimedia" in request_data:
        r_multimedia = request_data["multimedia"]
    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = insert(messages).values(
                    chat_id=r_chat_id,
                    sender_id=r_user_id,
                    message_text=r_message_text)
                result = conn.execute(stmt)

                message_id = result.inserted_primary_key[0]
                if r_multimedia is not None:
                    for file in r_multimedia:
                        if file:
                            media_file = post_media(PATH_TO_MESSAGES_MEDIA, r_chat_id, file)
                            stmt = insert(multimedia).values(
                                message_id=message_id,
                                multimedia=media_file)
                            conn.execute(stmt)
    except:
        json_response["status"] = False
        json_response["db_error"] = True
    return json.dumps(json_response, default=str)


@app.route('/message/get', methods=["POST"])
def get_messages():
    request_data = request.get_json()

    json_response = {"status": True,
                     "db_error": False,
                     "user_id": None,
                     "token": None
                     }

    r_chat_id = None
    r_mes_amount = None
    r_mes_skip = None

    json_response = auth_response(request_data, json_response)
    if json_response["status"]:
        r_user_id = request_data["user_id"]
    else:
        return json.dumps(json_response, default=str)

    if "chat_id" in request_data:
        r_chat_id = request_data["chat_id"]
    if "mes_amount" in request_data:
        r_mes_amount = request_data["mes_amount"]
    if "mes_skip" in request_data:
        r_mes_skip = request_data["mes_skip"]

    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = select([messages.c.message_id, messages.c.chat_id,
                               messages.c.sender_id, messages.c.sending_time,
                               messages.c.checked, messages.c.message_text,
                                multimedia.c.multimedia])
                stmt = stmt.select_from(messages)
                stmt = stmt.join(chat_users, chat_users.c.chat_id == messages.c.chat_id)
                stmt = stmt.join(multimedia, multimedia.c.message_id == messages.c.message_id, isouter=True)
                stmt = stmt.where(and_(messages.c.sender_id != chat_users.c.user_id,
                                       messages.c.chat_id == r_chat_id))
                stmt = stmt.order_by(messages.c.sending_time.desc())
                stmt = stmt.limit(r_mes_amount)
                stmt = stmt.offset(r_mes_skip)
                result = conn.execute(stmt).fetchall()
    except:
        json_response["status"] = False
        json_response["db_error"] = True
        return json.dumps(json_response, default=str)
    json_response["response"] = list()
    last_message_id = None
    for row in result:
        message_dict = dict(row)
        media_file = get_media(message_dict["multimedia"])
        message_dict["multimedia"] = list()

        if message_dict["message_id"] == last_message_id:
            json_response["response"][-1]["multimedia"].append(media_file)
        else:
            message_dict["multimedia"].append(media_file)
            last_message_id = message_dict["message_id"]
            json_response["response"].append(message_dict)

    return json.dumps(json_response, default=str)

@app.route('/user/picture', methods=["POST"])
def post_user_picture():
    request_data = request.get_json()
    json_response = {"status": True,
                     "db_error": False,
                     "user_id": None,
                     "token": None
                     }

    r_user_id = None
    r_picture = None

    json_response = auth_response(request_data, json_response)
    if json_response["status"]:
        r_user_id = request_data["user_id"]
    else:
        return json.dumps(json_response, default=str)

    if "picture" in request_data:
        r_picture = post_media(PATH_TO_USER_PICTURES, r_user_id, request_data["picture"])
    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = insert(user_pictures).values(
                    user_id=r_user_id,
                    picture=r_picture)
                conn.execute(stmt)
    except:
        json_response["status"] = False
        json_response["db_error"] = True
        return json.dumps(json_response, default=str)
    return json.dumps(json_response, default=str)


@app.route('/user/picture/get', methods=["POST"])
def get_user_picture():
    request_data = request.get_json()
    json_response = {"status": True,
                     "db_error": False
                     }

    r_user_id = None
    if "user_id" in request_data:
        r_user_id = request_data["user_id"]

    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = select([user_pictures])
                stmt = stmt.where(user_pictures.c.user_id == r_user_id)
                stmt = stmt.order_by(user_pictures.c.loaded.desc())
                result = conn.execute(stmt).fetchall()
    except:
        json_response["status"] = False
        json_response["db_error"] = True
        return json.dumps(json_response, default=str)

    template = dict(result[0])
    json_response["response"] = {
        "user_id": template["user_id"],
        "pictures": []
    }
    for row in result:
        user_pic_dict = dict(row)
        user_pic_dict["picture"] = get_media(user_pic_dict["picture"])
        json_response["response"]["pictures"].append({"picture": user_pic_dict["picture"],
                                                    "loaded": user_pic_dict["loaded"]})
    return json.dumps(json_response, default=str)


@app.route('/user/find', methods=["POST"])
def find_user():
    request_data = request.get_json()
    json_response = {"status": True,
                     "db_error": False,
                     "user_id": None,
                     "token": None
                     }
    r_username = None

    if "username" in request_data:
        r_username = request_data["username"].lower()

    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = select([users.c.user_id, users.c.username, users.c.nickname, users.c.bio,
                               user_pictures.c.picture, user_pictures.c.loaded])
                stmt = stmt.select_from(users.join(user_pictures, user_pictures.c.user_id == users.c.user_id, isouter=True))
                stmt = stmt.where(func.lower(users.c.username) == r_username)
                stmt = stmt.order_by(user_pictures.c.loaded.desc())
                stmt = stmt.limit(1)
                result = conn.execute(stmt).fetchall()
    except:
        json_response["status"] = False
        json_response["db_error"] = True
        return json.dumps(json_response, default=str)

    if len(result) > 0:
        template = dict(result[0])
        json_response["response"] = [{
            "user_id": template["user_id"],
            "username": template["username"],
            "nickname": template["nickname"],
            "bio": template["bio"],
            "picture": get_media(template["picture"])
        }]
    else:
        json_response["response"] = [{
            "user_id": None,
            "username": None,
            "nickname": None,
            "bio": None,
            "picture": None
        }]

    return json.dumps(json_response, default=str)


@app.route('/user/', methods=["POST"])
def get_user_info():
    request_data = request.get_json()
    json_response = {"status": True,
                     "db_error": False,
                     "user_id": None,
                     "token": None
                     }
    r_user_id = None
    if "user_id" in request_data:
        r_user_id = request_data["user_id"]

    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = select([users.c.user_id, users.c.username, users.c.nickname, users.c.bio,
                               user_pictures.c.picture, user_pictures.c.loaded])
                stmt = stmt.select_from(users.join(user_pictures, user_pictures.c.user_id == users.c.user_id, isouter=True))
                stmt = stmt.where(users.c.user_id == r_user_id)
                stmt = stmt.order_by(user_pictures.c.loaded.desc())
                result = conn.execute(stmt).fetchall()
    except:
        json_response["status"] = False
        json_response["db_error"] = True
        return json.dumps(json_response, default=str)

    template = dict(result[0])
    json_response["response"] = {
        "user_id": template["user_id"],
        "username": template["username"],
        "nickname": template["nickname"],
        "bio": template["bio"],
        "pictures": []
    }
    for row in result:
        users_dict = dict(row)
        users_dict["picture"] = get_media(users_dict["picture"])
        json_response["response"]["pictures"].append({"picture": users_dict["picture"],
                                                    "loaded": users_dict["loaded"]})

    return json.dumps(json_response, default=str)


@app.route('/user/session', methods=["PUT"])
def logout():
    request_data = request.get_json()
    json_response = {"status": True,
                     "db_error": False,
                     "user_id": None,
                     "token": None
                     }

    r_user_id = None
    r_token = None

    json_response = auth_response(request_data, json_response)
    if json_response["status"]:
        r_user_id = request_data["user_id"]
        r_token = request_data["token"]
    else:
        return json.dumps(json_response, default=str)

    try:
        with db.engine.connect().execution_options(autocommit=True) as conn:
            with conn.begin():
                stmt = update(user_sessions).where(and_(
                    user_sessions.c.token == r_token,
                    user_sessions.c.user_id == r_user_id
                    )).values(active=0)
                conn.execute(stmt)
    except:
        json_response["status"] = False
        json_response["db_error"] = True
        return json.dumps(json_response, default=str)
    return json.dumps(json_response, default=str)

