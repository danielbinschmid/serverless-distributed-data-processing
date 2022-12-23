## Generate Data using the TPC-H benchmark

Download the TPC-H generation tool from GitHub with the following command
```
git clone https://github.com/electrum/tpch-dbgen.git
cd tpch-dbgen
```

Build dbgen
```
make
```

Inside the dbgen repository, execute
```
./dbgen -T c -s 40
```

This will produce a customers.tbl data table of approximately 1 GB.

If you want to experiment with a dataset of different sizes, you can change the scaling factor (-s). The initial customer table is 24 MB, so if you want to generate a 5GB dataset, you will need to use a scaling factor of 200.

### Convert to CSV

```
g++ -o tbl_to_csv tbl_to_csv.cpp
./tbl_to_csv customer.tbl customer.csv
```

### Verify Results

You can also use the mean_acctbal.cpp file to verify that your pipeline returns the correct results.
```
g++ -o mean_acctbal mean_acctbal.cpp
./mean_acctbal customer.csv
```
