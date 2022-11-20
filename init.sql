create schema babynames;
drop table if exists babynames."d_Genders" cascade ;
drop table if exists babynames."d_States" cascade;
drop table if exists babynames."d_Names" cascade;
drop table if exists babynames."f_NameCounts" cascade;
DROP TYPE if exists  gender cascade ;
drop index if exists  babynames."IX_f_raw_StateNames_NameState";

CREATE TYPE gender AS ENUM ('M', 'F', 'other');
create table babynames."d_Genders"
(
    "Id"   smallserial             not null
        constraint "PK_Genders"
        primary key,
    "Gender" gender
);
insert into  babynames."d_Genders"
("Id","Gender") values
(0,'M'),
(1,'F'),
(2,'other')
on conflict ("Id") do update set "Gender" = excluded."Gender";

create table babynames."d_States"
(
    "Id"      smallserial             not null
        constraint "PK_States"
            primary key,
    "State" varchar(15),
    "StateLong" varchar(31)
);
INSERT INTO babynames."d_States" ("Id", "State", "StateLong") VALUES
(0, 'anonymized','anonymized'),
(1, 'AK','Alaska'),
 (2, 'AL','Alabama'),
 (3, 'AR','Arkansas'),
 (4, 'AZ','Arizona'),
 (5, 'CA','California'),
 (6, 'CO','Colorado'),
 (7, 'CT','Connecticut'),
 (8, 'DC','District of Columbia'),
 (9, 'DE','Delaware'),
 (10, 'FL','Florida'),
 (11, 'GA','Georgia'),
 (12, 'HI','Hawaii'),
 (13, 'IA','Iowa'),
 (14, 'ID','Idaho'),
 (15, 'IL','Illinois'),
 (16, 'IN','Indiana'),
 (17, 'KS','Kansas'),
 (18, 'KY','Kentucky'),
 (19, 'LA','Louisiana'),
 (20, 'MA','Massachusetts'),
 (21, 'MD','Maryland'),
 (22, 'ME','Maine'),
 (23, 'MI','Michigan'),
 (24, 'MN','Minnesota'),
 (25, 'MO','Missouri'),
 (26, 'MS','Mississippi'),
 (27, 'MT','Montana'),
 (28, 'NC','North Carolina'),
 (29, 'ND','North Dakota'),
 (30, 'NE','Nebraska'),
 (31, 'NH','New Hampshire'),
 (32, 'NJ','New Jerse'),
 (33, 'NM','New Mexico'),
 (34, 'NV','Nevada'),
 (35, 'NY','New York'),
 (36, 'OH','Ohio'),
 (37, 'OK','Oklahoma'),
 (38, 'OR','Oregon'),
 (39, 'PA','Pennsylvania'),
 (40, 'RI','Rhode Island'),
 (41, 'SC','South Carolina'),
 (42, 'SD','South Dakota'),
 (43, 'TN','Tennessee'),
 (44, 'TX','Texas'),
 (45, 'UT','Utah'),
 (46, 'VA','Virginia'),
 (47, 'VT','Vermont'),
 (48, 'WA','Washington'),
 (49, 'WI','Wisconsin'),
 (50, 'WV', 'West Virginia'),
 (51, 'WY','Wyoming')
 on conflict ("Id") do update set "State" = excluded."State", "StateLong" = excluded."StateLong";


create table babynames."d_Names"
(
    "Id"       bigserial             not null
        constraint "PK_Names"
            primary key,
    "Name" varchar(255),
    "Origin" varchar(255) default 'unknown'
);

create table babynames."f_NameCounts"
(
    "Id"      bigserial             not null
        constraint "PK_NameCounts"
            primary key,
    "NameId"   bigint
        constraint "FK_f_NameCounts_d_Names_NameId"
            references babynames."d_Names",
    "Date"   date,
    "GenderId" smallint
        constraint "FK_f_NameCounts_d_Genders_GenderId"
            references babynames."d_Genders",
    "StateId"  smallint
        constraint "FK_f_NameCounts_d_States_StateId"
            references babynames."d_States",
    "Count"  bigint
);

create index "IX_f_raw_StateNames_NameState"
    on babynames."raw_StateNames" ("Name", "State");


create index "IX_f_NameCounts_NameId"
    on babynames."f_NameCounts" ("NameId");
create index "IX_f_NameCounts_GenderId"
    on babynames."f_NameCounts" ("GenderId");
create index "IX_f_NameCounts_StateId"
    on babynames."f_NameCounts" ("StateId");
create index "IX_f_NameCounts_Id"
    on babynames."f_NameCounts" ("Id");

create index "IX_d_Names_Name"
    on babynames."d_Names" ("Name");
create index "IX_d_States_State"
    on babynames."d_States" ("State");