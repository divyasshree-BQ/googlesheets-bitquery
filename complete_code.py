import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json

# Setup the connection to Google Sheets
def authenticate_gsheets():
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
             "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('google_sheets.json', scope)
    client = gspread.authorize(creds)
    spreadsheet = client.create("test_bitquery_transfer")
    # Get the first worksheet
    worksheet = spreadsheet.sheet1
    spreadsheet.share('divyasshree@bitquery.io', perm_type='user', role='writer')
    return worksheet

# Fetch data from Bitquery API
def fetch_bitquery_data():
    url = "https://graphql.bitquery.io/"
    headers = {'Content-Type': 'application/json', 'X-API-KEY': 'keyy'}
    query = """
    {
        ethereum(network: ethereum) {
            transfers(
                options: {desc: "block.height", limit: 10, offset: 0}
                time: {since: "2024-04-30T09:59:06.000Z", till: "2024-04-30T10:29:06.999Z"}
                amount: {gt: 0}
            ) {
                block {
                    timestamp {
                        time(format: "%Y-%m-%d %H:%M:%S")
                    }
                    height
                }
                sender {
                    address
                    annotation
                }
                receiver {
                    address
                    annotation
                }
                currency {
                    address
                    symbol
                }
                amount
                amount_usd: amount(in: USD)
                transaction {
                    hash
                }
                external
            }
        }
    }
    """
    response = requests.post(url, json={'query': query}, headers=headers)
    if response.status_code == 200:
        # print(response.text)
        return json.loads(response.text)
    else:
        raise Exception("Query failed and return code is {}. {}".format(response.status_code, response.text))

# Write data to Google Sheets
def update_sheet(worksheet, data):
    transfers = data['data']['ethereum']['transfers']
    for i, transfer in enumerate(transfers, start=2):  # Assuming headers are in the first row
        worksheet.update(
            range_name=f'A{i}:D{i}', 
            values=[[
                transfer['currency']['symbol'],
                transfer['amount'],
                transfer['sender']['address'],
                transfer['receiver']['address']
            ]]
        )

# # Read data from Google Sheets
# def read_sheet(worksheet):
#     # Read all values in the worksheet
#     data = worksheet.get_all_values()
#     return data

def main():
    worksheet = authenticate_gsheets()
    data = fetch_bitquery_data()
    update_sheet(worksheet, data)

if __name__ == "__main__":
    main()
