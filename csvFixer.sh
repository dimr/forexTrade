cut -d, -f1,2,3,4,5,6,7 EURUSD.csv >test.csv
sed 's/,/ /'<test.csv>test2.csv
