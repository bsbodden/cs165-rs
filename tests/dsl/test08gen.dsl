-- Min,Max
--
-- Min
-- SELECT min(col1) FROM tbl2;
a1=min(db1.tbl2.col1)
print(a1)
--
--
-- SELECT min(col1) FROM tbl2 WHERE col1 >= 1622 AND col1 < 2622;
s1=select(db1.tbl2.col1,1622,2622)
f1=fetch(db1.tbl2.col1,s1)
m1=min(f1)
print(m1)
--
-- SELECT min(col2) FROM tbl2 WHERE col1 >= 1622 AND col1 < 2622;
f2=fetch(db1.tbl2.col2,s1)
m2=min(f2)
print(m2)
--
--
-- Max
-- SELECT max(col1) FROM tbl2;
a2=max(db1.tbl2.col1)
print(a2)
--
--
-- SELECT max(col1) FROM tbl2 WHERE col1 >= 1622 AND col1 < 2622;
s21=select(db1.tbl2.col1,1622,2622)
f21=fetch(db1.tbl2.col1,s21)
m21=max(f21)
print(m21)
--
-- SELECT max(col2) FROM tbl2 WHERE col1 >= 1622 AND col1 < 2622;
f22=fetch(db1.tbl2.col2,s21)
m22=max(f22)
print(m22)
