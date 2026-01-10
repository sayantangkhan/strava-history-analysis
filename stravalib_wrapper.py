import json
import os
from datetime import datetime

from stravalib import Client


def initialize_client() -> Client:
    token_path = os.path.join("secrets", "token.json")

    ## Load the stored credentials
    with open(token_path, "r") as f:
        token_refresh = json.load(f)

    ## We then check if they're too old
    current_timestamp = int(datetime.now().timestamp())
    if current_timestamp >= token_refresh["expires_at"]:
        ## We use the secret and refresh token to get new access token
        auth_path = os.path.join("secrets", "authentication.json")
        with open(auth_path) as f:
            auth_details = json.load(f)
        client = Client()
        token_response = client.refresh_access_token(
            client_id=auth_details["client_id"],
            client_secret=auth_details["client_secret"],
            refresh_token=token_refresh["refresh_token"],
        )
        token_refresh = token_response
        with open(token_path, "w") as f:
            json.dump(token_refresh, f)

    ## Create the client using the access token
    client = Client(
        access_token=token_refresh["access_token"],
        refresh_token=token_refresh["refresh_token"],
        token_expires=token_refresh["expires_at"],
    )

    return client
