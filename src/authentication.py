import json, hmac, datetime, hashlib

"""Functions to create the authentication message, provided by CEX """

def create_signature(key, secret):  # (string key, string secret) 
    timestamp = int(datetime.datetime.now().timestamp())  # UNIX timestamp in seconds
    string = "{}{}".format(timestamp, key)
    return timestamp, hmac.new(secret.encode(), string.encode(), hashlib.sha256).hexdigest()

def auth_request(key, secret):
    timestamp, signature = create_signature(key, secret)
    return json.dumps({'e': 'auth',
        'auth': {'key': key, 'signature': signature, 'timestamp': timestamp,}, 
                       'oid': 'auth', 
                      })


                      