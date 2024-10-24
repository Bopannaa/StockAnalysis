using DataFrames
using  CSV
using  Dates

function  getFilterdDataframe(path)
    df = CSV.read(path, DataFrame)
    df[!, :Date] = Date.(df[!, :Date], "dd-mm-yyyy")
    df = dropmissing!(ifelse.(df .== "null", missing, df))
    df.Open = parse.(Float32, df.Open)
    df.High = parse.(Float32, df.High)
    df.Low = parse.(Float32, df.Low)
    df.Close = parse.(Float32, df.Close)
    df.Volume = parse.(Float32, df.Volume)
    df = df[!, [:Date, :Open, :Close, :High, :Low, :Volume]]
    return df
end

function filterYear(dataframe, year)
    df_temp = dataframe[dataframe[!,:Date] .>= Date(year,1,1),:]
    df_temp = df_temp[df_temp[!,:Date] .< Date(year + 1,1,1),:]
    dfsize = size(df_temp)
    if dfsize[1] == 0 return nothing
    else return df_temp end
end

function getDiffDataframeOfEachYear(dataframe)
    df = dataframe
    df = df[!, [:Date, :Open, :High]]
    df.Diff = df.High - df.Open
    dfs = [filterYear(df, i) for  i in 2000:2023]
    [df.Date = Dates.format.(df[!,:Date], "mm/dd") for df in dfs]
    dfs = [df[!, [:Date, :Diff]] for df in dfs]
    [rename!(dfs[i], :Diff => string(1999+i)) for i in 1:24]
    df_a = outerjoin(dfs[1], dfs[2], on = :Date)
    for i in 3:24
        df_a = outerjoin(df_a, dfs[i], on = :Date)
    end
    return df_a
    sort!(df_a, :Date)
end


df = getFilterdDataframe("data/nifty50/RELIANCE.csv")
