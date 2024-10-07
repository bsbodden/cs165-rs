-- Addition
--
-- SELECT col2+col3 FROM tbl2 WHERE col1 >= -132 AND col1 < -112;
s11=select(db1.tbl2.col1,-132,-112)
f11=fetch(db1.tbl2.col2,s11)
f12=fetch(db1.tbl2.col3,s11)
a11=add(f11,f12)
print(a11)
