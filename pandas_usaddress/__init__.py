#!python3
"""
"""

import csv
from pathlib import Path
from typing import Any, Iterator, Sequence, Union, cast
from pkg_resources import resource_filename
import pandas as pd
import numpy as np
import usaddress

def __csvLoader(csvFile:Union[str, Path]) -> Iterator:
    """
    Loads a csv file and returns an iterator of the rows.
    """
    resourcePath = Path(resource_filename(__name__, str(csvFile)))
    if not resourcePath.is_file():
        raise FileNotFoundError(f"`{resourcePath.resolve()}` does not exist.")
    with open(resourcePath, 'r', encoding= "utf-8") as f:
        reader = csv.reader(f)
    return reader

# Reference dictionaries
abb_dict, num_dict, str_dict = (dict(__csvLoader(_x)) for _x in ('abbreviations.csv', 'numbered_streets.csv', 'street_numbers.csv'))


usaddress_fields = [
                    "AddressNumber",
                    "AddressNumberPrefix",
                    "AddressNumberSuffix",
                    "BuildingName",
                    "CornerOf",
                    "IntersectionSeparator",
                    "LandmarkName",
                    "NotAddress",
                    "OccupancyType",
                    "OccupancyIdentifier",
                    "PlaceName",
                    "Recipient",
                    "StateName",
                    "StreetName",
                    "StreetNamePreDirectional",
                    "StreetNamePreModifier",
                    "StreetNamePreType",
                    "StreetNamePostDirectional",
                    "StreetNamePostModifier",
                    "StreetNamePostType",
                    "SubaddressIdentifier",
                    "SubaddressType",
                    "USPSBoxGroupID",
                    "USPSBoxGroupType",
                    "USPSBoxID",
                    "USPSBoxType",
                    "ZipCode"
                    ]

def usaddress_field_creation(x,i):
    try:
        return x[0][i]
    except:
        return None

def trim(x:Any) -> Union[str, None]:
    """
    Original `trim` implementation for pandas-usaddress
    """
    x = str(x)
    x = x.split()
    x = ' '.join(x)
    if len(x) == 0:
        return None
    else:
        return x

def taggit(x):
    try:
        return usaddress.tag(x)
    except Exception:
        return None

def lowercase(x:str) -> Union[str, None]:
    """
    Original `lowercase` implementation for pandas-usaddress
    """
    try:
        return x.lower()
    except:
        return None

def tagColumn(column:pd.Series) -> pd.Series:
    """
    Tags a column of addresses.
    """
    strCol = cast(pd.Series[str], column.fillna("").astype(str).str.replace(r'[^\w\s\-]', '').str.replace(r"\s+", " ").str.strip().str.lower())
    return strCol.replace("", None)

def tag(dfa:pd.DataFrame, address_columns:Sequence[str], granularity:str='full', standardize:bool= False) -> pd.DataFrame:
    df = dfa.copy()
    df['odictaddress'] = ""
    for i in address_columns:
        df[i].fillna('', inplace=True)
    df['odictaddress'] = df['odictaddress'].str.cat(df[address_columns].astype(str), sep=" ", na_rep='')
    df['odictaddress'] = df['odictaddress'].str.replace(r'[^\w\s\-]','')
    df['odictaddress'] = df['odictaddress'].apply(lambda x: trim(x))
    df['odictaddress'] = df['odictaddress'].apply(lambda x: lowercase(x))
    df['odictaddress'] = df['odictaddress'].apply(lambda x: taggit(x))


    for i in usaddress_fields:
        df[i] = df['odictaddress'].apply(lambda x: usaddress_field_creation(x,i))

    df = df.drop(columns='odictaddress')
    # standardize parameter
    if not isinstance(standardize, bool):
        raise TypeError("standardize must be a boolean.")
    if standardize:
        df["StreetNamePreDirectional"] = df["StreetNamePreDirectional"].apply(lambda x: abb_dict.get(x, x))
        df["StreetNamePreType"] = df["StreetNamePreType"].apply(lambda x: abb_dict.get(x, x))
        df["StreetNamePostDirectional"] = df["StreetNamePostDirectional"].apply(lambda x: abb_dict.get(x, x))
        df["StreetNamePostType"] = df["StreetNamePostType"].apply(lambda x: abb_dict.get(x, x))
        df["StreetName"] = df["StreetName"].apply(lambda x: num_dict.get(x, x))
        df["AddressNumber"] = df["AddressNumber"].apply(lambda x: str_dict.get(x, x))



    if granularity=='full':
        pass
    elif granularity == 'high':
        df.drop(columns=[
                "AddressNumberPrefix",
                "AddressNumberSuffix",
                "CornerOf",
                "IntersectionSeparator",
                "LandmarkName",
                "NotAddress",
                "USPSBoxGroupID",
                "USPSBoxGroupType",
            ],inplace=True)
    elif granularity == 'medium':
        df['StreetNamePrefix'] = ''
        df['StreetNamePrefix'] = df['StreetNamePrefix'].str.cat(df[['StreetNamePreModifier', 'StreetNamePreType']], sep=" ", na_rep='')
        df['StreetNamePrefix'] = df['StreetNamePrefix'].apply(lambda x: trim(x))

        df['StreetNameSuffix'] = ''
        df['StreetNameSuffix'] = df['StreetNameSuffix'].str.cat(df[['StreetNamePostType', 'StreetNamePostModifier']], sep=" ", na_rep='')
        df['StreetNameSuffix'] = df['StreetNameSuffix'].apply(lambda x: trim(x))

        df['USPSBox'] = ''
        df['USPSBox'] = df['USPSBox'].str.cat(df[['USPSBoxType', 'USPSBoxID']], sep=" ", na_rep='')
        df['USPSBox'] = df['USPSBox'].apply(lambda x: trim(x))

        df['OccupancySuite'] = ''
        df['OccupancySuite'] = df['OccupancySuite'].str.cat(df[['OccupancyType', 'OccupancyIdentifier']], sep=" ", na_rep='')
        df['OccupancySuite'] = df['OccupancySuite'].apply(lambda x: trim(x))

        df.drop(columns=[
                "Recipient",
                "BuildingName",
                "SubaddressType",
                "SubaddressIdentifier",
                "AddressNumberPrefix",
                "AddressNumberSuffix",
                "CornerOf",
                "IntersectionSeparator",
                "LandmarkName",
                "NotAddress",
                "USPSBoxGroupID",
                "USPSBoxGroupType",
                "StreetNamePreModifier",
                "StreetNamePreType",
                "StreetNamePostType",
                "StreetNamePostModifier",
                "USPSBoxType",
                "USPSBoxID",
                "OccupancyType",
                "OccupancyIdentifier"
            ],inplace=True)

    elif granularity=='low':
        df['StreetTag'] = ""
        df['StreetTag'] = df['StreetTag'].str.cat(df[
            [
                #"AddressNumber",
                "StreetNamePreDirectional",
                "StreetNamePreModifier",
                "StreetNamePreType",
                "StreetName",
                "StreetNamePostType",
                "StreetNamePostModifier",
                "StreetNamePostDirectional",
                "USPSBoxType",
                "USPSBoxID",
                "OccupancyType",
                "OccupancyIdentifier"
            ]], sep=" ", na_rep='')
        df['StreetTag'] = df['StreetTag'].apply(lambda x: trim(x))


        df.drop(columns=[
            "Recipient",
            "BuildingName",
            "SubaddressType",
            "SubaddressIdentifier",
            "AddressNumberPrefix",
            "AddressNumberSuffix",
            "CornerOf",
            "IntersectionSeparator",
            "LandmarkName",
            "NotAddress",
            "USPSBoxGroupID",
            "USPSBoxGroupType",
            "StreetNamePreModifier",
            "StreetNamePreType",
            "StreetNamePostType",
            "StreetNamePostModifier",
            "USPSBoxType",
            "USPSBoxID",
            "OccupancyType",
            "OccupancyIdentifier",
            #"AddressNumber",
            "StreetNamePreDirectional",
            "StreetName",
            "StreetNamePostDirectional"
            ],inplace=True)
    elif granularity=='single':
        df['SingleLine'] = ""
        df['SingleLine'] = df['SingleLine'].str.cat(df[
            [
                "AddressNumber",
                "StreetNamePreDirectional",
                "StreetNamePreModifier",
                "StreetNamePreType",
                "StreetName",
                "StreetNamePostType",
                "StreetNamePostModifier",
                "StreetNamePostDirectional",
                "USPSBoxType",
                "USPSBoxID",
                "OccupancyType",
                "OccupancyIdentifier",
                "PlaceName",
                "StateName",
                "ZipCode"
            ]], sep=" ", na_rep='')
        df['SingleLine'] = df['SingleLine'].apply(lambda x: trim(x))

        df.drop(columns=[
                "Recipient",
                "BuildingName",
                "SubaddressType",
                "SubaddressIdentifier",
                "AddressNumberPrefix",
                "AddressNumberSuffix",
                "CornerOf",
                "IntersectionSeparator",
                "LandmarkName",
                "NotAddress",
                "USPSBoxGroupID",
                "USPSBoxGroupType",
                "StreetNamePreModifier",
                "StreetNamePreType",
                "StreetNamePostType",
                "StreetNamePostModifier",
                "USPSBoxType",
                "USPSBoxID",
                "OccupancyType",
                "OccupancyIdentifier",
                "AddressNumber",
                "StreetNamePreDirectional",
                "StreetName",
                "StreetNamePostDirectional",
                "PlaceName",
                "StateName",
                "ZipCode"
            ],inplace=True)

    df = df.replace({'None': np.nan, 'none': np.nan, 'nan': np.nan, 'NaN': np.nan, None: np.nan, '': np.nan}).copy()
    return df
