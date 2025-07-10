import time

time.sleep(0.001)  # avoid zero execution time check :-)
"""
# region client-with-user-certificates
query_string = f"tls=true&userCertFile={cert_file_path}&userKeyFile={key_file_path}"
connection_string = f"kurrentdb://{username}:{password}@{endpoint}?{query_string}"
client = KurrentDBClient(uri=connection_string)
# endregion client-with-user-certificates
"""
