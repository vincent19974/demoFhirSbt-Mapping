import os

USER = os.environ.get('username')
PASSWORD = os.environ.get('password')

with open('/tmp/input.json',"r") as f:
    print(f.read())
    f.close()

print(USER)
print(PASSWORD)
