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
    spreadsheet = client.create("test_bitquery_DEXTrades")
    # Get the first worksheet
    worksheet = spreadsheet.sheet1
    spreadsheet.share('divyasshree@bitquery.io', perm_type='user', role='writer')
    print(f"Spreadsheet URL: {spreadsheet.url}")  # Print URL for direct access
    return worksheet

# Fetch data from Bitquery API using OAuth
def fetch_bitquery_data():
    url = "https://streaming.bitquery.io/graphql"
    headers = {'Authorization': 'Bearer ory_at_eEbFJHmd64CBPm1cm58V3y0qjYm1LfFtTGiF6PkZEvA.47AuZYHst9XUD_ESAQBRhWBjew0YQRrZfLJmHoAJntQ', 'Content-Type': 'application/json'}
    query = """
    {
      EVM(dataset: archive, network: eth) {
        DEXTrades(limit: {count: 5}, orderBy: {descending: Block_Time}) {
          Block {
            Number
            Time
          }
          Transaction {
            From
            To
            Hash
          }
          Trade {
            Buy {
              Amount
              Buyer
              Currency {
                Name
                Symbol
                SmartContract
              }
              Seller
              Price
              PriceInUSD
            }
            Sell {
              Amount
              Buyer
              Currency {
                Name
                SmartContract
                Symbol
              }
              Seller
              Price
            }
            Dex {
              ProtocolFamily
              ProtocolName
              SmartContract
              Pair {
                SmartContract
              }
            }
          }
        }
      }
    }
    """
    response = requests.post(url, json={'query': query}, headers=headers)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        raise Exception("Query failed and return code is {}. {}".format(response.status_code, response.text))

# Write data to Google Sheets
def update_sheet(worksheet, data):
    trades = data['data']['EVM']['DEXTrades']
    for i, trade in enumerate(trades, start=2):  # Assuming headers are in the first row
        values = [
            trade['Block']['Time'],
            trade['Transaction']['Hash'],
            trade['Trade']['Buy']['Amount'],
            trade['Trade']['Buy']['Currency']['Symbol'],
            trade['Trade']['Sell']['Amount'],
            trade['Trade']['Sell']['Currency']['Symbol'],
            trade['Trade']['Dex']['ProtocolName']
        ]
        # Update the sheet using named arguments
        worksheet.update(range_name=f'A{i}:G{i}', values=[values])  


def main():
    worksheet = authenticate_gsheets()
    data = fetch_bitquery_data()
    update_sheet(worksheet, data)

if __name__ == "__main__":
    main()
