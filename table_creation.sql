Create table If Not Exists Offers (
	Id varchar(128) primary key,
	Beds float,
	Baths float,
	Surface bigint,
	street varchar(128),
	city varchar(50),
	state varchar(50),
	country varchar(50),
	Posted Date,
	Monthly_rent float
)