#!python3
"""
"""
# cSpell:words usaddress, subaddress, fillna

import csv
from pathlib import Path
from typing import Any, Dict, Iterator, Sequence, Union, cast
from pkg_resources import resource_filename
import pandas as pd
import numpy as np
import usaddress

def __csvDictLoader(csvFile:Union[str, Path]) -> Dict:
    """
    Loads a csv file and returns an iterator of the rows.
    """
    resourcePath = Path(resource_filename(__name__, str(csvFile)))
    if not resourcePath.is_file():
        raise FileNotFoundError(f"`{resourcePath.resolve()}` does not exist.")
    with open(resourcePath, 'r', encoding= "utf-8") as f:
        reader = csv.reader(f)
        _dict = dict(reader)
    return _dict

# Reference dictionaries
abb_dict, num_dict, str_dict = (dict(__csvDictLoader(_x)) for _x in ('abbreviations.csv', 'numbered_streets.csv', 'street_numbers.csv'))


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
    except Exception:
        return None

def taggit(x):
    try:
        return usaddress.tag(x)
    except Exception:
        return None

def removeExtraWhitespace(column:pd.Series) -> pd.Series:
    """
    Removes extra whitespace from a column.
    """
    return column.str.replace(r"\s+", " ").str.strip()

def cleanColumn(column:pd.Series) -> pd.Series:
    """
    Cleans a column of addresses.
    """
    strCol = cast(pd.Series, removeExtraWhitespace(column.fillna("").astype(str).str.replace(r'[^\w\s\-]', '')).str.lower())
    return strCol.replace("", None)

def tag(dfa:pd.DataFrame, address_columns:Sequence[str], granularity:str='full', standardize:bool= False) -> pd.DataFrame:
    """
    Tags a DataFrame of addresses.
    """
    df = dfa.copy()
    df['oDictAddress'] = ""
    for i in address_columns:
        df[i].fillna('', inplace=True)
    df['oDictAddress'] = cleanColumn(df['oDictAddress'].str.cat(df[address_columns].astype(str), sep=" ", na_rep=''))
    df['oDictAddress'] = df['oDictAddress'].apply(taggit)

    for i in usaddress_fields:
        df[i] = df['oDictAddress'].apply(lambda x: usaddress_field_creation(x,i))

    df = df.drop(columns='oDictAddress')
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

    def createConcatenatedColumn(columns:Sequence[str]) -> pd.Series:
        """
        """
        return removeExtraWhitespace(pd.Series("", index= list(range(len(df)))).str.cat(df[columns], sep= " ", na_rep= ""))


    if granularity=='full':
        # No further work necessary
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
        df['StreetNamePrefix'] = createConcatenatedColumn(['StreetNamePreModifier', 'StreetNamePreType'])
        df['StreetNameSuffix'] = createConcatenatedColumn(['StreetNamePostType', 'StreetNamePostModifier'])
        df['USPSBox'] = createConcatenatedColumn(['USPSBoxType', 'USPSBoxID'])
        df['OccupancySuite'] = createConcatenatedColumn(['OccupancyType', 'OccupancyIdentifier'])
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
            ],inplace= True)

    elif granularity=='low':
        df['StreetTag'] = createConcatenatedColumn(
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
            ])

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
            ], inplace=True)

    elif granularity=='single':
        df['SingleLine'] = createConcatenatedColumn(
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
            ])

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
    else:
        raise ValueError("Granularity must be one of 'full', 'high', 'medium', 'low', 'single'")
    return df.replace({'None': np.nan, 'none': np.nan, 'nan': np.nan, 'NaN': np.nan, None: np.nan, '': np.nan})


if __name__ == "__main__":
    print("This module is not runnable. Please import `tag` from this module instead.")
