import requests
import json
import pandas as pd


def read_sp_data(url, max_loop=1000):
    """
    Imports a dataset from information.saintpaul.gov using odata connection into a Pandas dataframe
        Args:
            url (str): Odata url/endpoint
            max_loop (int): Max number of loops to grab next rowset
    """
    all_sets = []

    # loop count to prevent infinite looping.
    loop_count = 1

    while loop_count < max_loop:
        # retrieve data
        temp_data = requests.get(url)
        # parse json to list/dict
        temp_data = json.loads(temp_data.content)
        # append to list of dataframes
        all_sets.append(pd.DataFrame.from_dict(temp_data['value']))

        # next iteration
        if '@odata.nextLink' in temp_data:
            url = temp_data['@odata.nextLink']
            loop_count += 1
        else:
            break

    # append all dataframes to one dataframe then return
    return pd.concat(all_sets)


# Saint Paul Crime Data
# https://information.stpaul.gov/Public-Safety/Crime-Incident-Report-Dataset/gppb-g9cg



crime_data = read_sp_data("https://information.stpaul.gov/api/odata/v4/gppb-g9cg")
accident_data = read_sp_data("https://information.stpaul.gov/api/odata/v4/bw92-5h94")
