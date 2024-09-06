import numpy as np
from datetime import datetime
from collections import defaultdict


def timestamp2datetime_lists(lst:list[int]) -> list[datetime]:
    """
    Takes a list of timestamps (in seconds since epoch) and converts it to a 
    list of datetime objects.
    """
    return [datetime.fromtimestamp(ts) for ts in lst]


def separate_yearly_dates(datetime_list: list[datetime]) -> dict[int, 
                                                                 list[datetime]]:
    """
    Separates a list of datetime objects by year, returning a dictionary where 
    the keys are years and the values are lists of datetime objects from that 
    year.
    """
    grouped_years= defaultdict(list)
    for dt in datetime_list:
        grouped_years[dt.year].append(dt)

    return dict(grouped_years)


def date_comparison(dates1:list[datetime], 
                    dates2:list[datetime], 
                    dates_error=10) -> dict[list]:
    """
    Compares two lists of datetime objects and returns a dictionary with matched
    and unmatched dates. Dates are considered a match if their difference in 
    days is within `dates_error`.
    
    Parameters:
    - dates1        : List of datetime objects from the first set.
    - dates2        : List of datetime objects from the second set.
    - dates_error   : Maximum allowed difference in days between two matching 
                      dates (default is 10).
    
    Returns:
    - A dictionary containing:
        - 'Matched'       : List of tuples (difference in days, date from dates1
                            , matching date from dates2)
        - 'Only Manual'   : List of dates from dates1 that had no match in 
                            dates2
        - 'Only Estimated': List of dates from dates2 that had no match in 
                            dates1
    """
    
    def within_days(dtes1:datetime, dtes2:datetime) ->int:
        return abs((dtes2 - dtes1).days)

    matching = []
    unmatched_md = []
    
    for dt1 in dates1:
        single_match = []
        
        for dt2 in dates2:
            diff = within_days(dt1, dt2)
            if diff <= dates_error:
                single_match.append([diff, dt1, dt2])
        
        if single_match:
            if len(single_match) > 1:
                diff_list = [match[0] for match in single_match]
                soonest = min(diff_list)
                min_index = diff_list.index(soonest)
                matching.append(single_match[min_index])
            else:
                matching.append(single_match[0])
        else:
            unmatched_md.append(dt1)
    
    # Get matched dates from dates2
    matched_estimated = [match[2] for match in matching]
    
    # Calculate unmatched dates in dates2
    unmatched_ed = list(set(dates2) - set(matched_estimated))

    return {
        'Matched': matching,
        'Only Manual': unmatched_md,
        'Only Estimated': unmatched_ed,
    }