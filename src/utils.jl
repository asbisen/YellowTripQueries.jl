
"""
    DataFrame(r::YellowQueryResult)

Convenience function to return DataFrame from YellowQueryResult
"""
DataFrame(r::YellowQueryResult) = r.df
