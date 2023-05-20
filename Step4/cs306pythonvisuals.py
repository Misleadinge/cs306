# -*- coding: utf-8 -*-
"""
Spyder Editor
"""

# Import libs
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to the database
cnx = mysql.connector.connect(user='root', password='Tb843749',
                              host='127.0.0.1',
                              database='cs306')

# Define a SQL queries to fetch the data
query1 = "SELECT * FROM gdp_and_emissions"
query2 = "SELECT * FROM emissions_and_ozone"
query3 = "SELECT * FROM gdp_and_pollution"
query4 = "SELECT * FROM population_and_emissions"
query5 = "SELECT * FROM wind_and_airpollution"
query6 = "SELECT * FROM gdp_population_wind_energy"

# Queries for each view for Turkey
query7 = "SELECT * FROM emissions_and_ozone WHERE Code = 'TUR'"
query8 = "SELECT * FROM gdp_and_emissions WHERE Code = 'TUR'"
query9 = "SELECT * FROM gdp_and_pollution WHERE Code = 'TUR'"
query10 = "SELECT * FROM population_and_emissions WHERE Code = 'TUR'"
query12 = "SELECT * FROM gdp_population_wind_energy WHERE Code = 'TUR'"

# Use pandas to execute the query and store the result in a DataFrame
df1 = pd.read_sql_query(query1, cnx)
df2 = pd.read_sql_query(query2, cnx)
df3 = pd.read_sql_query(query3, cnx)
df4 = pd.read_sql_query(query4, cnx)
df5 = pd.read_sql_query(query5, cnx)
df6 = pd.read_sql_query(query6, cnx)

# dataframes for Turkey
df7 = pd.read_sql_query(query7, cnx)
df8 = pd.read_sql_query(query8, cnx)
df9 = pd.read_sql_query(query9, cnx)
df10 = pd.read_sql_query(query10, cnx)
df12 = pd.read_sql_query(query12, cnx)

# Close the connection
cnx.close()

# Scatter plot for GDP and emissions
plt.figure(figsize=(10,6))
sns.scatterplot(data=df1, x='GDP', y='Annual_CO2_emissions', hue='Country')
plt.title('Relation between GDP and Annual CO2 emissions')
plt.xlabel('GDP')
plt.ylabel('Annual CO2 emissions')

# Move the legend to below the plot
plt.legend(bbox_to_anchor=(0.5, -0.1), loc='upper center', borderaxespad=0., ncol=3)

plt.show()


# Line Chart for emissions and ozone
df2_filtered = df2[df2['Year'] == 2015] #adjust year
plt.figure(figsize=(10,6))
for country in df2_filtered['Country'].unique():
    country_data = df2_filtered[df2_filtered['Country'] == country]
    plt.plot(country_data['Annual_CO2_emissions'], country_data['Ozone_concentration_StateofGlobalAir'], label=country)
plt.title('Relation between Annual CO2 emissions and Ozone concentration')
plt.xlabel('Annual CO2 emissions')
plt.ylabel('Ozone concentration')
plt.legend(bbox_to_anchor=(0.5, -0.1), loc='upper center', borderaxespad=0., ncol=3)
plt.show()

# Bar plot for gdp and pollution
df3_filtered = df3[df3['Year'] == 2015] #adjust year
plt.figure(figsize=(10,6))
sns.barplot(data=df3_filtered, x='Country', y='Deaths_Air_pollution_Percent')
plt.title('Deaths due to Air Pollution per Country')
plt.xlabel('Country')
plt.ylabel('Deaths_Air_pollution_Percent')
plt.show()

# Column Chart (using barplot for vertical bars) for population and emissions
df4_filtered = df4[df4['Year'] == 2015] #adjust year
plt.figure(figsize=(10,6))
sns.barplot(data=df4_filtered, x='Country', y='Population')
plt.title('Population per Country')
plt.xlabel('Country')
plt.ylabel('Population')
plt.show()

# Pie chart for wind and air pollution (showing proportion of wind energy per capita)
df5_filtered = df5[df5['Year'] == 2015] #adjust year
wind_sum = df5_filtered['Wind_per_capita_kWh_equivalent'].sum()
df5_filtered['wind_proportion'] = df5_filtered['Wind_per_capita_kWh_equivalent'] / wind_sum
plt.figure(figsize=(12, 8))  # Increase the figure size to 12x8 inches
plt.pie(df5_filtered['wind_proportion'], labels = df5_filtered['Country'], autopct='%1.1f%%')
plt.title('Proportion of Wind Energy per capita by Country')
plt.show()

# Area Chart for gdp, population and wind energy
df6_filtered = df6[df6['Year'] == 2015] #adjust year
plt.figure(figsize=(10,6))
plt.fill_between(df6_filtered['GDP'], df6_filtered['Population'], color="skyblue", alpha=0.4)
plt.plot(df6_filtered['GDP'], df6_filtered['Population'], color="Slateblue", alpha=0.6)
plt.title('Relation between GDP and Population')
plt.xlabel('GDP')
plt.ylabel('Population')
plt.show()



# Create visualizations for Turkey
# 1. Line chart for annual CO2 emissions over the years
plt.figure(figsize=(12,8))
sns.lineplot(data=df7, x='Year', y='Annual_CO2_emissions')
plt.title('Annual CO2 emissions Over Years in Turkey')
plt.show()

# 2. Bar chart for fossil fuel consumption over the years
plt.figure(figsize=(12,8))
sns.barplot(data=df8, x='Year', y='Fossil_fuels_TWh')
plt.title('Fossil Fuels Consumption Over Years in Turkey')
plt.show()

# 3. Scatter plot for GDP vs Ozone concentration
plt.figure(figsize=(10,6))
sns.scatterplot(data=df9, x='GDP', y='Ozone_concentration_StateofGlobalAir')
plt.title('GDP vs Ozone Concentration in Turkey')
plt.show()

# 4. Area chart for Population and Annual CO2 emissions
df10.set_index('Year')[['Population', 'Annual_CO2_emissions']].plot(kind='area', alpha=0.4, stacked=False)
plt.title('Population and Annual CO2 emissions Over Years in Turkey')
plt.show()


# 6. Column chart for Wind per capita kWh equivalent over the years
plt.figure(figsize=(12,8))
df12.set_index('Year')['Wind_per_capita_kWh_equivalent'].plot(kind='bar')
plt.title('Wind per capita kWh Equivalent Over Years in Turkey')
plt.show()
