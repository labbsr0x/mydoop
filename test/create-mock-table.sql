create table table_mock(time_column_1 timestamp,column_1 varchar(200), column_2 varchar(200), column_3 bigint, column_4 bigint, column_5 bigint, column_6 bigint, column_7 bigint)

insert into table_mock values('2018-04-26 12:00:00','COL--1', 'COL--2', 1, 2, 3, 4, 44);
insert into table_mock values('2018-04-26 12:00:00','COL--1', 'COL--2', 11, 22, 33, 44, 44);
insert into table_mock values('2018-04-26 21:00:00','COL--1', 'COL--2', 111, 222, 333, 444, 555);

insert into table_mock values('2018-04-26 01:00:00','COL--11', 'COL--22', 1, 2, 3, 4, 5);
insert into table_mock values('2018-04-26 12:00:00','COL--111', 'COL--222', 11, 22, 33, 44, 44);
insert into table_mock values('2018-04-26 21:00:00','COL--11', 'COL--22', 111, 222, 333, 444, 44);
