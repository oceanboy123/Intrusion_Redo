import numpy as np
from datetime import datetime

def timestamp2datetime_lists(lst:list[int]) -> list[datetime]:
    """
    Takes a list of timestapms and converst it to a list of datetimes
    """
    datetime_list:list[datetime] = [datetime.fromtimestamp(ts) for ts in lst]
    return datetime_list

def separate_yearly_dates(datetime_list:list[datetime]) -> dict[list]:
    """
    Separates a list of datetimes by year by separating indices
    """
    years_extracted:list = np.unique([dt.year for dt in datetime_list])

    grouped_years:dict[list] = {year: [] for year in years_extracted}
    for i in datetime_list:
        grouped_years[i.year].append(i)

    return grouped_years

def date_comparison(dates1:list[datetime], dates2:list[datetime], dates_error=10) -> dict[list]:
    """
    Compares datetime lists for similar (within self.dates_error) dates
    """
    
    def within_days(dtes1:datetime, dtes2:datetime) ->int:
        calc = abs((dtes2 - dtes1).days)
        return calc

    matching = []
    unmatched_md = []

    for dt1 in dates1:
        found_match = False
        single_match = []

        for dt2 in dates2:
            diff = within_days(dt1, dt2)
            if diff <= dates_error:
                single_match.append([diff, dt1, dt2])
                found_match = True
                break
        # Intrusions Not Found
        if not found_match:
            unmatched_md.append(dt1)
        else:
            # Intrusions Found
            if len(single_match) > 1:
                diff_list = [match[0] for match in single_match]
                min_index = [idx for idx, value in enumerate(diff_list) if value == min(diff_list)]
                matching.append([single_match[min_index]])
            else:
                matching.append(single_match) 
    
    matching = [item for sublist in matching for item in sublist]
    matching_estimated = [sublist[2] for sublist in matching]

    set1 = set(dates2)
    set2 = set(matching_estimated)
    # Extra Intrusions
    unmatched_ed = list(set1-set2)

    return {
        'Matched':matching,
        'Only Manual':unmatched_md,
        'Only Estimated':unmatched_ed,
    }