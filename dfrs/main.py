from dfrspy import DataFrame, Series
s = Series(("a", "b"))
print(s)
df = DataFrame({"a": [1, 2, 3, 4], "b": [5, 6, 7, 8]})
print(f"{df["a"] + df["b"]}")
print(df)
a = df["a"]
print(a)
df["b"] = ["a", "b", "c", "d"]
print(df)
df["c"] = [3.14, 2.52, -1, 2]
print(df)
print(len(df))
newdf = DataFrame.from_csv("demo.csv")
print(newdf)
print(newdf["colC"] + df["a"])
