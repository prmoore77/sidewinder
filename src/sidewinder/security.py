import os
import click
import hashlib
from pathlib import Path
import json
from .constants import USER_LIST_FILENAME

# Constants
TOKEN_DELIMITER = ":"

# Setup the secret key - use an environment variable if set
SECRET_KEY = os.getenv("SECRET_KEY", "Not so secret")


def create_user(username: str,
                password: str,
                user_list_filename: str = USER_LIST_FILENAME,
                secret_key: str = SECRET_KEY
                ):
    user_list_file = Path(user_list_filename).resolve()
    if user_list_file.exists():
        with open(file=user_list_file, mode="r") as user_list_file_data:
            user_dict = json.loads(user_list_file_data.read())
    else:
        user_list_file.parent.mkdir(parents=True, exist_ok=True)
        user_dict = dict()

    password_hash = hashlib.sha256()
    password_hash.update(secret_key.encode())
    password_hash.update(password.encode())

    user_dict.update({username: password_hash.hexdigest()})

    with open(file=user_list_file, mode="w") as user_list_file_data:
        user_list_file_data.write(json.dumps(user_dict))

    print(f"User: {username} - successfully created - and info stored in file: {user_list_file.resolve()}")


def authenticate_user(user_list_filename: str,
                      username: str,
                      password: str,
                      secret_key: str
                      ):
    user_list_file = Path(user_list_filename).resolve()
    if user_list_file.exists():
        with open(file=user_list_file, mode="r") as user_list_file_data:
            user_dict = json.loads(user_list_file_data.read())
    else:
        raise RuntimeError(f"The user list file: {user_list_file.resolve()} does NOT exist!  Aborting.")

    password_hash = hashlib.sha256()
    password_hash.update(secret_key.encode())
    password_hash.update(password.encode())

    stored_user_password_hash = user_dict.get(username)
    if not stored_user_password_hash:
        print(f"User: {username} does NOT exist in the user list file: {user_list_file.resolve()} - Authentication failed.")
        return False
    elif password_hash.hexdigest() != stored_user_password_hash:
        print(f"Incorrect password or secret key supplied.  Authentication failed.")
        return False
    elif password_hash.hexdigest() == stored_user_password_hash:
        print(f"Authentication successful for user: {username}")
        return True


@click.command()
@click.option(
    "--user-list-filename",
    type=str,
    default=USER_LIST_FILENAME,
    show_default=True,
    required=True,
    help="The user dictionary file (in JSON) to store the created user data in.  This file should be used to start Sidewinder server afterward."
)
@click.option(
    "--username",
    type=str,
    required=True,
    help="The username of the user to create.  If a user with the username already exists, the program will just update that entry's password."
)
@click.option(
    "--password",
    type=str,
    required=True,
    help="The password of the user to create."
)
@click.option(
    "--secret-key",
    type=str,
    default=SECRET_KEY,
    required=True,
    help="The secret key used to salt the password hash.  The same key value MUST be used for running Sidewinder server as well!"
)
def click_create_user(username: str,
                      password: str,
                      user_list_filename: str = USER_LIST_FILENAME,
                      secret_key: str = SECRET_KEY):
    create_user(**locals())


@click.command()
@click.option(
    "--user-list-filename",
    type=str,
    default="security/user_list.json",
    show_default=True,
    required=True,
    help="The user dictionary file (in JSON) to store the created user data in.  This file should be used to start Sidewinder server afterward."
)
@click.option(
    "--username",
    type=str,
    required=True,
    help="The username of the user authenticate."
)
@click.option(
    "--password",
    type=str,
    required=True,
    help="The password of the user to authenticate."
)
@click.option(
    "--secret-key",
    type=str,
    default=SECRET_KEY,
    required=True,
    help="The secret key used to salt the password hash.  The same key value MUST be used for running Sidewinder server as well!"
)
def click_authenticate_user(user_list_filename: str,
                            username: str,
                            password: str,
                            secret_key: str):
    authenticate_user(**locals())
