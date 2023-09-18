import clickhouse_connect #to get data from clickhouse
import pandas as pd
import time
import numpy as np
from fpdf import FPDF
from calendar import monthrange
from datetime import date, timedelta, datetime
import asyncio
import nest_asyncio
from telethon import TelegramClient, functions, types
import pytz

#result is the query from clickhouse
#data manipulation
df = pd.DataFrame(result.result_rows, columns=result.column_names)
df = df.astype({'revenue':'int', 'sold_items':'int'})
data = f'{datetime.today().replace(day=1).date().strftime("%d.%m")} - {(date.today() - timedelta(days=1)).strftime("%d.%m")}' #because the script is running in the morning - the date is yesterday
current_date = f'{(date.today() - timedelta(days=1)).strftime("%d.%m.%Y")}'
yesterday = f'{(date.today() - timedelta(days=2)).strftime("%d")}'
day = f'{(date.today() - timedelta(days=1)).strftime("%d")}'
day_in_month = monthrange(int(f'{datetime.today().replace(day=1).date().strftime("%Y")}'), int(f'{datetime.today().replace(day=1).date().strftime("%m")}'))[1]

df2 = pd.DataFrame(result2.result_rows, columns=result2.column_names)
df2['amount_total'] = df2['amount_total'].astype(int)
sum_amount_total = df2.query('source_sale == "Online"')['amount_total'].sum() #the reveneu from web store in every shop with sourse_sale as Online, so we need to upload row Web store with new_reveneu
row_index = df2.index[df2['union_name'] == 'Web store'].tolist()[0]
df2.at[row_index, 'amount_total'] = sum_amount_total
df2.at[row_index, 'source_sale'] = 'Online'

df2 = df2[df2.source_sale != 'Online']
df2 = df2[['union_name', 'amount_total']].sort_values(by=['amount_total'], ascending=False)
df2 = df2.rename(columns={'union_name': 'Shop', 'amount_total': 'Fact'})
df2['Feature'] = df2['Fact']/int(day)*day_in_month
df2['Feature'] = df2['Feature'].astype(int)
df2_plan = pd.read_excel('plan.xlsx')
df_today = df2.merge(df2_plan, on='Shop', how='left')
df_today['Plan'] = df_today['Plan'].astype(int)
df_today['Execution'] = round(df_today['Feature']/df_today['Plan']*100)
df_today['Execution'] = df_today['Execution'].astype(int)

fact = int(df_today['Plan'].sum())
plan = int(df_today['Fact'].sum())
faeture = int(fact/int(day)*day_in_month)
execution = int(prognoz/plan*100)
df_today.loc[len(df_today)]=['Sum', fact, faeture, plan, execution] 

class PDF(FPDF):
    def __init__(self):
        super().__init__('L', 'mm', 'A4')  # Set page size to reverse A4 (297x210mm)
        self.height = 5.5
        self.font_size = 10
        self.add_font('CenturyGothic', '', 'font/GOTHIC.ttf', uni=True)
        self.add_font('CenturyGothic', 'B', 'font/GOTHICB.ttf', uni=True)
        self.set_font('CenturyGothic', '', 23)
        self.page_number = 0


    def header(self):
        self.page_number += 1
        if self.page_number == 1:
            self.set_text_color(2, 101, 146)
            self.set_font('CenturyGothic', '', 11)  # Change the font size 
            self.cell(0, 5.5, data, border=0, ln=1, align='C')
        elif self.page_number == 2: 
            self.set_text_color(2, 101, 146)
            self.set_font('CenturyGothic', '', 11)  # Change the font size 
            self.cell(90, 7, '', border=0) # Add an empty cell to create space on the left
            self.cell(0, 7, current_date, border=0, ln=1, align='L')  # Align the date in the center
           

    def table(self, df, style): 
        if style == 'style1': 
            self.widths = [120, 30, 40]
        # Calculate total width of the table 
            total_width = sum(self.widths) 

            # Calculate left margin needed to center the table 
            left_margin = (self.w - total_width) / 2 

            # Set left margin 
            self.set_left_margin(left_margin) 

            self.set_font('CenturyGothic', 'B', self.font_size + 1)  # Increase the font size for the column headers 
            for col in df.columns: 
                self.set_fill_color(253, 185, 21) 
                self.cell(self.widths[df.columns.get_loc(col)], self.height, col, border=0, fill=True, ln=False) 

            self.ln() 

            self.set_font('CenturyGothic', '', 8)  # Reset the font size to normal after the column headers 
            for row in df.values: 
                for i in range(len(row)): 
                    if isinstance(row[i], int) or isinstance(row[i], float): 
                        self.cell(self.widths[i], self.height, '{:,.0f}'.format(int(row[i])).replace(',', ' '), border=0, ln=False, align='R') 
                    else: 
                        text = str(row[i]) 
                        if i == 0 and len(text) > 67:  # If text in the first column is longer than 67 characters 
                            split_index = text.rfind(' ', 0, 67)  # Find the last space within the first 67 characters 
                            first_line = text[:split_index] 
                            second_line = text[split_index + 1:] 
                            self.cell(self.widths[i], self.height, first_line, border=0, ln=False) 
                            self.ln() 
                            self.cell(self.widths[i], self.height, second_line, border=0, ln=False) 
                        else: 
                            self.cell(self.widths[i], self.height, text, border=0, ln=False) 
                self.ln()           
            pass
        elif style == 'style2': 
            
            self.widths = [60, 27, 27, 27, 27, 37]

            self.set_font('CenturyGothic', 'B', self.font_size + 4.7)  # Increase the font size for the column headers 
            for i, col in enumerate(df.columns): 
                self.set_fill_color(253, 185, 21)
                if i != 0:  # Check if this is not the first column
                    align = 'R'
                else:
                    align = 'L'
                self.cell(self.widths[df.columns.get_loc(col)], self.height, col, border=0, fill=True, ln=False, align=align) 

            self.ln() 

            self.set_font('CenturyGothic', '', 7)  # Reset the font size to normal after the column headers 
            for i, row in enumerate(df.values):
                for j in range(len(row)):
                    if i == len(df.values) - 1:  # Check if this is the last row
                        self.set_fill_color(225, 225, 225)  # Set background color to red for the last row
                    else:
                        self.set_fill_color(255, 255, 255)
                    if isinstance(row[j], int) or isinstance(row[j], float):
                        if j == len(row) - 1 or j == len(row) - 2:
                            self.cell(self.widths[j], 4.25, '{:,.0f}%'.format(int(row[j])).replace(',', ' '), border=0, ln=False, align='R', fill=True)
                        else:
                            self.cell(self.widths[j], 4.25, '{:,.0f}'.format(int(row[j])).replace(',', ' '), border=0, ln=False, align='R', fill=True)
                    else:
                        text = str(row[j])
                        self.cell(self.widths[j], 4.25, text, border=0, ln=False, fill=True)
                self.ln()    

            pass
            
# Create PDF object and add page
pdf = PDF()
pdf.add_page()
pdf.table(df, 'style1')
pdf.add_page()
pdf.table(df_today, 'style2')

pdf.output(f'report {(date.today() - timedelta(days=1)).strftime("%d.%m")}.pdf') #save the report in desktop

#send to telegram
# Apply nest_asyncio to the current event loop
nest_asyncio.apply()

# Set your API credentials, phone number, and session file name
api_id = ''
api_hash = ''
phone_number = ''
session_file = 'session_name'
client = TelegramClient(session_file, api_id, api_hash)

# Define an async function to send the file
async def send_file():
    # Start the client
    async with client:
        # Get the user ID of the recipient
        user = await client.get_entity('')
        user_id = user.id

        # Send the file to the user
        file_path = f'belif {datetime.datetime.today().strftime("%d.%m")}.pdf'

        # Get current time in UTC
        now = datetime.datetime.now(pytz.timezone('UTC'))

        # Calculate next weekday at 5:00 UTC
        if now.weekday() < 5 and now.time() < datetime.time(5, 0):
            # If today is a weekday and it's not yet 10:00, schedule for today at 10:00
            scheduled_time = now.replace(hour=5, minute=30, second=0, microsecond=0)
        else:
            # Otherwise, schedule for next weekday at 10:00
            days_ahead = 0 if now.weekday() < 4 else 7 - now.weekday()
            scheduled_time = (now + datetime.timedelta(days=days_ahead)).replace(hour=5, minute=30, second=0, microsecond=0)

        await client.send_file(user_id, file=file_path, schedule=scheduled_time)

# Run the async function
asyncio.run(send_file())
