import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt

# file path of Otahuhu and Benmore Prices, '2010-01-01' to '2024-01-01'.
file_path = 'C:/Users/bryce/OneDrive - The University of Auckland/PersonalProjects/Query outputs/OTABEN_PRICES.csv'

# Use pandas to read the CSV file and store the data in a DataFrame.
df = pd.read_csv(file_path)
grouped_data = df.groupby('TradingDate')
result = grouped_data['DollarsPerMegawattHour'].mean()

# fig = px.scatter(df, x='x_column', y='y_column', title='Scatter Plot')



# Assuming 'result' is the DataFrame obtained after aggregating the data.
# Example of a bar plot for the aggregated data.
plt.bar(result['TradingDate'], result['DollarsPerMegawattHour'])
plt.xlabel('Group')
plt.ylabel('Sum of Numeric Column')
plt.title('Aggregated Data')
plt.show()