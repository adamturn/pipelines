# adam note:
#   check path to the db properties file in Loader.copy_to_db()
#   the Loader will automatically make sure the data is copied
#   to the appropriate table depending on props['environment'].

# standard library
import os
import glob
import shutil
import pickle
import base64
import zipfile
from io import BytesIO
from datetime import datetime
# third-party
from googleapiclient.discovery import build
# first-party
import conndb

# leftover from original Gmail API authentication:
# from google_auth_oauthlib.flow import InstalledAppFlow
# from google.auth.transport.requests import Request


class Loader(object):
    """Pipeline object that processes and loads data from the Gmail API.

    Initialize with Loader.construct() class method.

    Attributes:
        scopes (list[str]): defines the level of access that this program has to the Gmail account.
        creds: pickled Gmail API authentication token obtained from the Gmail API quickstart.
        extract_dir (str): temporary directory that holds the unzipped .csv files after they are unencoded.
        write_table (str): Postgres table where the data ends up; depends on database environment (DEV|PROD).
    """

    def __init__(self, temp_dir):
        """If Loader.scopes are modified, Loader.creds becomes invalid.

        If you must do this, first delete the local file 'token.pickle'.
        Then uncomment the relevant code in Loader.quickstart_authentication().
            ctrl-F: 'creds.invalid'
        You will be asked to authenticate by logging in to Gmail via Chrome.
        This will not work on a server so you will either need to do this by running this script locally
        or by getting a different credentials.json from the Gmail API quickstart.

        Arguments:
            temp_dir (str): passed from Loader.construct() class method.
        """
        self.scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.creds = None
        self.extract_dir = temp_dir
        self.write_table = None

    @classmethod
    def construct(cls):
        print("Constructing Loader object...")

        temp_dir = "/home/loader_module/temp_dir/"

        try:
            os.mkdir(temp_dir)
            print("{}> Temporary extraction dir at:\n\t".format(' '*2) + temp_dir)
        except FileExistsError:
            shutil.rmtree(temp_dir)
            os.mkdir(temp_dir)
            print("{}> Leftover extraction dir detected and destroyed at:\n{}".format(' '*2, ' '*4) + temp_dir)

        print("{}> Loader object constructed.".format(' '*2))

        return cls(temp_dir)

    def quickstart_authentication(self):
        """Securely connect to the Gmail account.

        Contains biolerplate code taken from the Gmail API quickstart.
        """
        print("Gmail account authentication in progress...")
        creds = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
                print("{}> Credentials found.".format(' '*2))

        # adam note:
        #   I have commented out the following part of the quickstart
        #   because there is no way for the user to manually log in to
        #   gmail via the server without browser tunneling which is
        #   inconvenient to set up. If the credentials ever become invalid,
        #   a manual review of this process should be done before generating
        #   new credentials.

        # # If there are no (valid) credentials available, let the user log in.
        # if not creds or not creds.valid:
        #     if creds and creds.expired and creds.refresh_token:
        #         creds.refresh(Request())
        #     else:
        #         flow = InstalledAppFlow.from_client_secrets_file('credentials.json', self.scopes)
        #         creds = flow.run_local_server(port=0)
        #     # Save the credentials for the next run
        #     with open('token.pickle', 'wb') as token:
        #         pickle.dump(creds, token)

        self.creds = creds
        print("{}> Authentication complete.".format(' '*2))

        return self

    def decode_and_extract(self, data, data_id):
        """Decodes message attachment file data and extracts .csv files.

        'data' comes to us from the Gmail API as an encoded string.
        First, we confirm that the data is encoded via UTF-8.
        Then we decode the data, unzip the file, and extract the .csv files.
        All internal .csv files appear to be named the same thing.
        To counter this: upon extraction, each file is immediately renamed to the 
        first 10 digits of its 'data_id' followed by the current datetime.now()
        to ensure that the files don't write over each other.

        Arguments:
            data: encoded file data
            data_id (str): id uniquely associated with accompanying data
        """
        print("Beginning decoding and extraction process for: {}".format(data_id[:10]))
        print("{}> Decoding data...".format(' '*2))
        str_csv = base64.urlsafe_b64decode(data.encode('UTF-8'))

        print("{}> Extracting data...".format(' '*2))
        with zipfile.ZipFile(BytesIO(str_csv), 'r') as zf:
            zf.extractall(self.extract_dir)
        filename_list = glob.glob(self.extract_dir + "*")
        latest_filename = max(file_list, key=os.path.getctime)
        os.rename(latest_filename, self.extract_dir + data_id[:10] + "_{}.csv".format(str(datetime.now())))

        print("{}> Decode and extraction complete.".format(' '*2))

        return None

    def copy_to_db(self):
        """Copies the extracted data to a table in our database.

        Accomplishes this at ~maximum~ speed by dynamically generating a shell script that copies data
        from the extracted .csv files to a Postgres temp table. An 'insert_timestamp' column is then
        generated precisely before copying the temp table to the permanent table in production.
        """
        props = conndb.parse_props("/home/config_files/config_dev.properties")

        temp_table_name = "x_xxx_xxxxxx_xxx"
        if props['environment'] == 'PROD':
            write_to_this_table = "xxxxxxxxx.xxx_xxxxxx_xxx"
        else:
            write_to_this_table = "xxx.xxx_xxx_xxxxxx_xxx_xxxx"
        print("Copying data to: {}".format(write_to_this_table))

        shell_script_filepath = self.extract_dir + "copy_csv_to_db.sh"
        print("{}> Dynamically generating script: {}".format(' '*2, shell_script_filepath))

        shell_script_payload = "#!/usr/bin/env/ sh \n\n"
        shell_script_payload += "/usr/bin/psql \"host={db_host} port={db_port} dbname={db_name} user={db_user} password={db_pass}\" << EOF \n\n".format(
            db_host=props['db_host'],
            db_user=props['db_user'],
            db_pass=props['db_password'],
            db_port=props['db_port'],
            db_name=props['db_name'],
        )
        shell_script_payload += "CREATE TEMP TABLE {temp_table_name} AS (SELECT fileid, filename, xxxxxxxxxxx, xxxxxxxxxxxxx, xxxxxxxx, xxxxxxxxxx, xxxxxxxxxx, xxxxxxxxxxxx, xxxxxxxxx, xxxxxxxx, xxxxx, xxxxxxxx, xxxxxxx, xxxxxx, xxxxxxxx, xxxxxxxxxxxxxxx, xxxxx, xxxxxxxxx, xxxxx, xxxxxx, xxxxxx, xxxxxx, xxxxxxxxxx, xxxxxxx FROM {write_to_this_table} LIMIT 0); \n".format(
            temp_table_name=temp_table_name,
            write_to_this_table=write_to_this_table
        )
        for filename in os.listdir(self.extract_dir):
            filepath = self.extract_dir + filename
            print("{}> Adding file: {}".format(' '*2, filepath))
            # adam request:
            #   please forgive the extremely long lines I think it actually makes this part easier to understand.
            shell_script_payload += "\\COPY {temp_table_name} FROM \'{filepath}\' WITH (FORMAT csv, DELIMITER \',\', HEADER true); \n\n".format(
                temp_table_name=temp_table_name,
                filepath=filepath
            )
            continue
        shell_script_payload += "ALTER TABLE {temp_table_name} ADD COLUMN insert_timestamp timestamp;\n".format(
            temp_table_name=temp_table_name
        )
        shell_script_payload += "UPDATE {temp_table_name} SET insert_timestamp = current_timestamp WHERE fileid IS NOT NULL;\n".format(
            temp_table_name=temp_table_name
        )
        shell_script_payload += "INSERT INTO {write_to_this_table} SELECT * FROM {temp_table_name}; \nEOF\n".format(
            write_to_this_table=write_to_this_table,
            temp_table_name=temp_table_name
        )
        print("{}> Payload: \n\n".format(' '*2), shell_script_payload)

        # create shell script
        with open(shell_script_filepath, "w+") as f:
            f.write(shell_script_payload)
        # that was awesome
        print("{}> Executing: {}".format(' ' * 2, shell_script_filepath))
        os.system("sh " + shell_script_filepath)
        print("{}> Success! All .csv files copied to: {}".format(' '*2, write_to_this_table))

        return None


def main():
    # constructs main data pipeline object
    pipeline = Loader.construct().quickstart_authentication()

    # builds Gmail API service
    service = build('gmail', 'v1', credentials=pipeline.creds)

    # searches Gmail for new messages labeled with 'xxx_xxxxxx_xxx'
    search_query = "label:xxx_xxxxxx_xxx newer_than:1d"
    print("Searching Gmail for new data...\n{}> {}".format(' '*2, search_query))
    results = service.users().messages().list(userId='me', q=search_query).execute()
    msgs = results['messages']
    msgs_ids = [msg['id'] for msg in msgs]

    for msg_id in msgs_ids:
        msg = service.users().messages().get(userId='me', id=msg_id).execute()
        parts = msg.get('payload').get('parts')

        # filters message parts such that the list only contains parts with zip file mimeTypes
        # a filename list can be obtained with the following comprehension: [part['filename'] for part in parts]
        parts = [part for part in parts if part['mimeType'] == 'application/zip']

        # based on what I've seen, 'parts' will only contain a single 'part' because each email only contains
        # a single zip file with data inside. however, this for loop is here just in case the email contains multiple
        # zipped files. process will break if the email contains both .csv and .zip files.
        for part in parts:
            attachment_id = part['body'].get('attachmentId')
            attachment = service.users().messages().attachments().get(
                userId='me', messageId=msg_id, id=attachment_id
            ).execute()
            data = attachment['data']
            pipeline.decode_and_extract(data, attachment_id)

            continue

        continue

    pipeline.copy_to_db()

    return None


if __name__ == '__main__':
    main()
