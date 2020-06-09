# Python 3.7.7
# adam note:
#   This script currently exports regional data features only from 2017.
#
#   For quickstart use, jump to 'def main' and add any additional Series IDs of interest.
#   Then, jump to 'my_path' and change that to a convenient location on your computer.
#   Finally, run the script! :)

# standard library
import math
# third-party
import pandas as pd
import datapungi_fed as dpf


class Wrangler(object):

    @staticmethod
    def annualize(dataframe):
        print("Annualizing monthly data...")
        tmp = dataframe[['year', 'county', 'feature', 'value']]
        tmp = tmp.groupby(['county', 'feature', 'year'])
        tmp = tmp.aggregate(func='mean')

        return tmp.reset_index()

    @staticmethod
    def long_to_wide(dataframe):
        print("Transforming long format to wide format...")
        tmp = dataframe.reset_index(drop=True)
        tmp = tmp.pivot_table(
            index=['year', 'county'],
            values='value',
            columns='feature'
        )

        return tmp.reset_index()

    @staticmethod
    def mean_replace(dataframe, threshold):
        """Drops threshold NaN cols and replaces remaining NaNs with average values.

        Args:
            dataframe (pd.DataFrame): to transform
            threshold (float): max (nans / rows) allowed for a feature

        Returns: pd.DataFrame, drops nan threshold cols and mean-replaces the rest
        """
        tmp = dataframe
        rows = tmp.shape[0]
        nan_threshold = math.ceil(rows * threshold)
        nans = tmp.isna().sum()

        drop_cols = list()
        for col in nans.index:
            if nans[col] > nan_threshold:
                drop_cols.append(col)
        print("Over NaN Threshold, Dropping:", drop_cols)
        tmp = tmp.drop(columns=drop_cols)

        return tmp.fillna(tmp.mean())

    @staticmethod
    def normalize_features(dataframe):
        """Scales all numeric features from 0 to 1."""
        print("Normalizing feature matrix...")
        tmp = dataframe
        feats = tmp.drop(columns=['year', 'county'])
        fmax = feats.max()
        fmin = feats.min() 
        # normalize the feature matrix
        feats = (feats - fmin) / (fmax - fmin)
        tmp[feats.columns] = feats

        return tmp

    @staticmethod
    def reduce_to_hpi(dataframe):
        print("Reducing dataframe to counties with recorded HPI...")
        tmp = dataframe
        print("  > Original shape:", tmp.shape)
        tmp = tmp[tmp['All-Transactions House Price Index'].isna() == False]
        print("  > Reduced shape:", tmp.shape)

        return tmp

    @staticmethod
    def transform_fresh_columns(dataframe):
        tmp = dataframe
        tmp['value'] = tmp['value'].astype(float)
        tmp['year'] = tmp['_date'].str[:4]
        tmp = tmp.rename(columns={'series_id': 'feature', 'region': 'county'})
        # tmp = tmp[tmp['year'] == '2017']

        return tmp[['year', 'county', 'feature', 'value']]


def pull_data_from_geodb(geodb, sids):
    """Gets data from the GeoFRED database.

    Pulls all the data into two dataframes based
    on whether the features are annual or monthly.

    Args:
        geodb: database object from datapungi_fed
        sids (list[str]): all of the series ids for the features we want

    Returns: tuple[pd.DataFrame] of the form (df_annual, df_month)
    """
    cutoff_start = pd.to_datetime('2009-01-01').date()  # start after mortgage crisis
    cutoff_end = pd.to_datetime('2017-12-31').date()  # last 'day' we have for hpi

    df_a = pd.DataFrame()  # for annual features
    df_m = pd.DataFrame()  # for monthly features

    # gets meta data for each feature from its series id, 
    # then uses meta data to pull all of the feature's regional data
    for sid in sids:
        print(f"Series ID: {sid}")

        print("  > Collecting meta data...")
        meta = geodb['meta'](series_id=sid).iloc[0, :]
        for col in ['min_date', 'max_date']:
            meta[col] = pd.to_datetime(meta[col]).date()

        # ensures min date isn't before cutoff start date
        if meta['min_date'] < cutoff_start:
            meta['min_date'] = cutoff_start
        # if min date starts after Jan 01, change min date to Jan 01 of the 
        # next year as this will remove potential for seasonal bias during annualization
        elif meta['min_date'] > pd.to_datetime(str(meta['min_date'].year) + "-01-01").date():
            meta['min_date'] = pd.to_datetime(str(meta['min_date'].year + 1) + "-01-01").date()

        # ensures max date isn't after cutoff end date
        if meta['max_date'] > cutoff_end:
            if meta['frequency'] == 'a':
                meta['max_date'] = pd.to_datetime('2017-01-01').date()
            else:
                meta['max_date'] = cutoff_end

        # checkdate!
        if meta['max_date'] < meta['min_date']:
            print(f"  > ALERT: NOT ENOUGH DATA, REMOVING {sid}...")
            continue

        print("  > Pulling dataframe...")
        data = geodb['data'](
            series_group=meta['series_group'],
            date=meta['max_date'],
            start_date=meta['min_date'],
            region_type=meta['region_type'],
            units=meta['units'],
            frequency=meta['frequency'],
            season=meta['season']
        )
        print(f"  > Min date: {meta['min_date']}")
        print(f"  > Max date: {meta['max_date']}")
        # swap series id for its feature name (change colname later)
        data['series_id'] = meta['title']

        print(f"  > Appending to df_{meta['frequency']}...")
        if meta['frequency'] == 'a':
            df_a = df_a.append(data)
        else:
            df_m = df_m.append(data)
        continue

    df_a = Wrangler.transform_fresh_columns(df_a)
    df_m = Wrangler.transform_fresh_columns(df_m)

    return df_a, df_m


def main():
    api_key = "f125f4c130e61d9f4ad5874aadfe07ff"
    geodb = dpf.data(api_key).geo

    # TODO: add all the Series IDs you want to this list
    series_ids = [
        "ATNHPIUS37081A",  # this is All-Transactions House Price Index
        "NCGUIL1URN",
        "PCPI37081",
        "MHICA06037A052NCEN",
        "MELIPRCOUNTY6059",
        "SUSCCOUNTY48113",
        "PRIINCCOU54037",
        "TOTLISCOUMM39049",
        "MWACL06037",
        "DESCMSA18140",
        "S1701ACS024510",
        "PENLISCOU51061",
        "PENRATYY51153",
        "HORACOUNTY54037",
        "MELIPRCOUNTY6059",
        "NIHHRACOUNTY51061",
        "HOWNRATEACS006073",
        "FLHVAC",
        "ACTLISCOU6085",
        "MELIPRCOUNTY6059",
        "MEDAONMACOUNTY48471",
        "MEDSQUFEE14460",
        "NEWLISCOUMM48113"
    ]

    df_a, df_m = pull_data_from_geodb(geodb, series_ids)

    df = df_a.append(Wrangler.annualize(df_m))
    df = Wrangler.long_to_wide(df)
    df = Wrangler.reduce_to_hpi(df)
    df = Wrangler.mean_replace(df, threshold=0.85)
    df = Wrangler.normalize_features(df)

    return df


if __name__ == "__main__":
    df = main()
    # TODO: alter this path for your local env
    my_path = "/home/adam/Documents/truist_2020/hackathon/features_2017.csv"
    print("Exporting clean data...")
    df.to_csv(my_path, index=False)
    print(f"\nData available at {my_path}\n")
    breakpoint()
