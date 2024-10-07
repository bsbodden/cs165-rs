-- Subtraction
--
-- SELECT col3-col2 FROM tbl2 WHERE col1 >= 3124 AND col1 < 3144;
s21=select(db1.tbl2.col1,3124,3144)
f21=fetch(db1.tbl2.col2,s21)
f22=fetch(db1.tbl2.col3,s21)
s21=sub(f22,f21)
print(s21)
