#!/usr/bin/env python3

import sqlite3
from pathlib import Path

from tabulate import tabulate


db_path = Path(__file__).resolve().parent / "reliable_rentals_part3.db"

schema_sql = """
PRAGMA foreign_keys = ON;

CREATE TABLE Outlet (
    OutletNumber INTEGER PRIMARY KEY,
    Address TEXT NOT NULL CHECK (trim(Address) <> ''),
    PhoneNumber TEXT NOT NULL CHECK (length(PhoneNumber) = 11 AND PhoneNumber NOT GLOB '*[^0-9]*'),
    FaxNumber TEXT CHECK (FaxNumber IS NULL OR (length(FaxNumber) = 11 AND FaxNumber NOT GLOB '*[^0-9]*')),
    FOREIGN KEY (OutletNumber) REFERENCES OutletStaffCoverage(OutletNumber) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE Staff (
    StaffNumber INTEGER PRIMARY KEY,
    FirstName TEXT NOT NULL CHECK (trim(FirstName) <> ''),
    LastName TEXT NOT NULL CHECK (trim(LastName) <> ''),
    HomeAddress TEXT NOT NULL CHECK (trim(HomeAddress) <> ''),
    HomePhoneNumber TEXT NOT NULL CHECK (length(HomePhoneNumber) = 11 AND HomePhoneNumber NOT GLOB '*[^0-9]*'),
    DOB TEXT NOT NULL CHECK (date(DOB) IS NOT NULL),
    Sex TEXT NOT NULL CHECK (Sex IN ('M', 'F', 'O')),
    DateJoined TEXT NOT NULL CHECK (date(DateJoined) IS NOT NULL AND date(DateJoined) >= date(DOB)),
    JobTitle TEXT NOT NULL CHECK (trim(JobTitle) <> ''),
    Salary REAL NOT NULL CHECK (Salary > 0),
    OutletNumber INTEGER NOT NULL,
    FOREIGN KEY (OutletNumber) REFERENCES Outlet(OutletNumber) ON DELETE RESTRICT,
    UNIQUE (OutletNumber, StaffNumber)
);

CREATE TABLE OutletStaffCoverage (
    OutletNumber INTEGER PRIMARY KEY,
    StaffNumber INTEGER NOT NULL,
    FOREIGN KEY (OutletNumber) REFERENCES Outlet(OutletNumber) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (OutletNumber, StaffNumber) REFERENCES Staff(OutletNumber, StaffNumber) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE Vehicle (
    RegistrationNumber TEXT NOT NULL PRIMARY KEY CHECK (trim(RegistrationNumber) <> ''),
    Model TEXT NOT NULL CHECK (trim(Model) <> ''),
    Make TEXT NOT NULL CHECK (trim(Make) <> ''),
    EngineSize REAL NOT NULL CHECK (EngineSize > 0),
    Capacity INTEGER NOT NULL CHECK (Capacity >= 1),
    CurrentMileage INTEGER NOT NULL CHECK (CurrentMileage >= 0),
    DailyHireRate REAL NOT NULL CHECK (DailyHireRate > 0),
    OutletNumber INTEGER NOT NULL,
    FOREIGN KEY (OutletNumber) REFERENCES Outlet(OutletNumber) ON DELETE RESTRICT
);

CREATE TABLE Client (
    ClientNumber INTEGER PRIMARY KEY,
    FirstName TEXT NOT NULL CHECK (trim(FirstName) <> ''),
    LastName TEXT NOT NULL CHECK (trim(LastName) <> ''),
    HomeAddress TEXT NOT NULL CHECK (trim(HomeAddress) <> ''),
    PhoneNumber TEXT NOT NULL CHECK (length(PhoneNumber) = 11 AND PhoneNumber NOT GLOB '*[^0-9]*'),
    DateOfBirth TEXT NOT NULL CHECK (date(DateOfBirth) IS NOT NULL),
    DrivingLicenseNumber TEXT NOT NULL UNIQUE CHECK (trim(DrivingLicenseNumber) <> '')
);

CREATE TABLE HireAgreement (
    HireNumber INTEGER PRIMARY KEY,
    StartDate TEXT NOT NULL CHECK (date(StartDate) IS NOT NULL),
    EndDate TEXT CHECK (EndDate IS NULL OR (date(EndDate) IS NOT NULL AND date(EndDate) >= date(StartDate))),
    MileageBefore INTEGER NOT NULL CHECK (MileageBefore >= 0),
    MileageAfter INTEGER CHECK (MileageAfter IS NULL OR MileageAfter >= MileageBefore),
    ClientFirstNameSnapshot TEXT NOT NULL CHECK (trim(ClientFirstNameSnapshot) <> ''),
    ClientLastNameSnapshot TEXT NOT NULL CHECK (trim(ClientLastNameSnapshot) <> ''),
    ClientAddressSnapshot TEXT NOT NULL CHECK (trim(ClientAddressSnapshot) <> ''),
    ClientPhoneSnapshot TEXT NOT NULL CHECK (length(ClientPhoneSnapshot) = 11 AND ClientPhoneSnapshot NOT GLOB '*[^0-9]*'),
    VehicleModelSnapshot TEXT NOT NULL CHECK (trim(VehicleModelSnapshot) <> ''),
    VehicleMakeSnapshot TEXT NOT NULL CHECK (trim(VehicleMakeSnapshot) <> ''),
    ClientNumber INTEGER NOT NULL,
    RegistrationNumber TEXT NOT NULL,
    FOREIGN KEY (ClientNumber) REFERENCES Client(ClientNumber) ON DELETE RESTRICT,
    FOREIGN KEY (RegistrationNumber) REFERENCES Vehicle(RegistrationNumber) ON DELETE RESTRICT
);

CREATE TRIGGER ValidateStaffDatesInsert
BEFORE INSERT ON Staff
BEGIN
    SELECT
        CASE
            WHEN date(NEW.DOB) >= date('now')
            THEN RAISE(ABORT, 'Staff DOB must be earlier than the current date.')
        END;
    SELECT
        CASE
            WHEN date(NEW.DateJoined) >= date('now')
            THEN RAISE(ABORT, 'Staff DateJoined must be earlier than the current date.')
        END;
END;

CREATE TRIGGER EnsureOutletCoverageOnStaffInsert
AFTER INSERT ON Staff
WHEN NOT EXISTS (
    SELECT 1
    FROM OutletStaffCoverage
    WHERE OutletNumber = NEW.OutletNumber
)
BEGIN
    INSERT INTO OutletStaffCoverage (OutletNumber, StaffNumber)
    VALUES (NEW.OutletNumber, NEW.StaffNumber);
END;

CREATE TRIGGER ReassignOutletCoverageBeforeStaffDelete
BEFORE DELETE ON Staff
WHEN EXISTS (
    SELECT 1
    FROM OutletStaffCoverage
    WHERE OutletNumber = OLD.OutletNumber
      AND StaffNumber = OLD.StaffNumber
)
BEGIN
    SELECT
        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM Staff
                WHERE OutletNumber = OLD.OutletNumber
                  AND StaffNumber <> OLD.StaffNumber
            )
            THEN RAISE(ABORT, 'Each outlet must keep at least one staff member.')
        END;
    UPDATE OutletStaffCoverage
    SET StaffNumber = (
        SELECT StaffNumber
        FROM Staff
        WHERE OutletNumber = OLD.OutletNumber
          AND StaffNumber <> OLD.StaffNumber
        ORDER BY StaffNumber
        LIMIT 1
    )
    WHERE OutletNumber = OLD.OutletNumber;
END;

CREATE TRIGGER ReassignOutletCoverageBeforeStaffTransfer
BEFORE UPDATE OF OutletNumber ON Staff
WHEN OLD.OutletNumber <> NEW.OutletNumber
  AND EXISTS (
      SELECT 1
      FROM OutletStaffCoverage
      WHERE OutletNumber = OLD.OutletNumber
        AND StaffNumber = OLD.StaffNumber
  )
BEGIN
    SELECT
        CASE
            WHEN NOT EXISTS (
                SELECT 1
                FROM Staff
                WHERE OutletNumber = OLD.OutletNumber
                  AND StaffNumber <> OLD.StaffNumber
            )
            THEN RAISE(ABORT, 'Each outlet must keep at least one staff member.')
        END;
    UPDATE OutletStaffCoverage
    SET StaffNumber = (
        SELECT StaffNumber
        FROM Staff
        WHERE OutletNumber = OLD.OutletNumber
          AND StaffNumber <> OLD.StaffNumber
        ORDER BY StaffNumber
        LIMIT 1
    )
    WHERE OutletNumber = OLD.OutletNumber;
END;

CREATE TRIGGER EnsureOutletCoverageOnStaffTransfer
AFTER UPDATE OF OutletNumber ON Staff
WHEN OLD.OutletNumber <> NEW.OutletNumber
  AND NOT EXISTS (
      SELECT 1
      FROM OutletStaffCoverage
      WHERE OutletNumber = NEW.OutletNumber
  )
BEGIN
    INSERT INTO OutletStaffCoverage (OutletNumber, StaffNumber)
    VALUES (NEW.OutletNumber, NEW.StaffNumber);
END;

CREATE TRIGGER ValidateStaffDatesUpdate
BEFORE UPDATE OF DOB, DateJoined ON Staff
BEGIN
    SELECT
        CASE
            WHEN date(NEW.DOB) >= date('now')
            THEN RAISE(ABORT, 'Staff DOB must be earlier than the current date.')
        END;
    SELECT
        CASE
            WHEN date(NEW.DateJoined) >= date('now')
            THEN RAISE(ABORT, 'Staff DateJoined must be earlier than the current date.')
        END;
END;

CREATE TRIGGER ValidateClientDateOfBirthInsert
BEFORE INSERT ON Client
BEGIN
    SELECT
        CASE
            WHEN date(NEW.DateOfBirth) >= date('now')
            THEN RAISE(ABORT, 'Client DateOfBirth must be earlier than the current date.')
        END;
END;

CREATE TRIGGER ValidateClientDateOfBirthUpdate
BEFORE UPDATE OF DateOfBirth ON Client
BEGIN
    SELECT
        CASE
            WHEN date(NEW.DateOfBirth) >= date('now')
            THEN RAISE(ABORT, 'Client DateOfBirth must be earlier than the current date.')
        END;
END;

CREATE TRIGGER PreventVehicleHireOverlapInsert
BEFORE INSERT ON HireAgreement
BEGIN
    SELECT
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM HireAgreement
                WHERE RegistrationNumber = NEW.RegistrationNumber
                  AND date(NEW.StartDate) <= date(COALESCE(EndDate, '9999-12-31'))
                  AND date(StartDate) <= date(COALESCE(NEW.EndDate, '9999-12-31'))
            )
            THEN RAISE(ABORT, 'Vehicle hire periods cannot overlap.')
        END;
END;

CREATE TRIGGER PreventVehicleHireOverlapUpdate
BEFORE UPDATE ON HireAgreement
BEGIN
    SELECT
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM HireAgreement
                WHERE RegistrationNumber = NEW.RegistrationNumber
                  AND HireNumber <> NEW.HireNumber
                  AND date(NEW.StartDate) <= date(COALESCE(EndDate, '9999-12-31'))
                  AND date(StartDate) <= date(COALESCE(NEW.EndDate, '9999-12-31'))
            )
            THEN RAISE(ABORT, 'Vehicle hire periods cannot overlap.')
        END;
END;

CREATE TRIGGER PreventLastStaffDelete
BEFORE DELETE ON Staff
BEGIN
    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM Staff WHERE OutletNumber = OLD.OutletNumber) = 1
            THEN RAISE(ABORT, 'Each outlet must keep at least one staff member.')
        END;
END;

CREATE TRIGGER PreventLastStaffTransfer
BEFORE UPDATE OF OutletNumber ON Staff
WHEN OLD.OutletNumber <> NEW.OutletNumber
BEGIN
    SELECT
        CASE
            WHEN (SELECT COUNT(*) FROM Staff WHERE OutletNumber = OLD.OutletNumber) = 1
            THEN RAISE(ABORT, 'Each outlet must keep at least one staff member.')
        END;
END;
"""

if db_path.exists():
    db_path.unlink()

connection = sqlite3.connect(db_path)
connection.execute("PRAGMA foreign_keys = ON")
connection.executescript(schema_sql)

connection.executemany(
    "INSERT INTO Outlet (OutletNumber, Address, PhoneNumber, FaxNumber) VALUES (?, ?, ?, ?);",
    [
        (1, "101 King Street, Glasgow", "01415550101", "01415550199"),
        (2, "22 Princes Street, Edinburgh", "01315550202", "01315550299"),
        (3, "8 Union Square, Aberdeen", "01224550303", None),
        (4, "55 Tay Road, Dundee", "01382550404", None),
        (5, "14 High Street, Stirling", "01786550505", "01786550599"),
    ],
)

connection.executemany(
    """
    INSERT INTO Staff (
        StaffNumber,
        FirstName,
        LastName,
        HomeAddress,
        HomePhoneNumber,
        DOB,
        Sex,
        DateJoined,
        JobTitle,
        Salary,
        OutletNumber
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """,
    [
        (1, "Maria", "Lopez", "12 Birch Road, Glasgow", "07700900101", "1988-05-14", "F", "2018-03-01", "Manager", 42000.00, 1),
        (2, "David", "Chen", "44 Elm Street, Edinburgh", "07700900202", "1990-09-21", "M", "2019-06-15", "Assistant Manager", 36500.00, 2),
        (3, "Priya", "Patel", "9 Cedar Avenue, Aberdeen", "07700900303", "1992-02-08", "F", "2020-01-20", "Rental Agent", 29500.00, 3),
        (4, "James", "Walker", "71 Pine Close, Dundee", "07700900404", "1985-11-30", "M", "2017-08-11", "Mechanic", 33200.00, 4),
        (5, "Sarah", "Ahmed", "3 Willow Lane, Stirling", "07700900505", "1994-07-12", "F", "2021-04-05", "Rental Agent", 28750.00, 5),
    ],
)

connection.executemany(
    """
    INSERT INTO Vehicle (
        RegistrationNumber,
        Model,
        Make,
        EngineSize,
        Capacity,
        CurrentMileage,
        DailyHireRate,
        OutletNumber
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?);
    """,
    [
        ("GLA-1001", "Corolla", "Toyota", 1.8, 5, 24500, 48.00, 1),
        ("GLA-1006", "Tucson", "Hyundai", 2.0, 5, 8700, 74.00, 1),
        ("EDI-2002", "Focus", "Ford", 1.5, 5, 18300, 46.00, 2),
        ("ABD-3003", "CR-V", "Honda", 2.0, 5, 31000, 72.00, 3),
        ("DUN-4004", "Micra", "Nissan", 1.2, 5, 12200, 39.00, 4),
    ],
)

connection.executemany(
    """
    INSERT INTO Client (
        ClientNumber,
        FirstName,
        LastName,
        HomeAddress,
        PhoneNumber,
        DateOfBirth,
        DrivingLicenseNumber
    )
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """,
    [
        (1, "Emma", "Johnson", "9 Rose Street, Glasgow", "07800111111", "1995-01-17", "DLA123456A"),
        (2, "Noah", "Smith", "27 Castle Road, Edinburgh", "07800222222", "1989-06-02", "DLB234567B"),
        (3, "Olivia", "Brown", "18 Harbour View, Aberdeen", "07800333333", "1993-09-25", "DLC345678C"),
        (4, "Liam", "Wilson", "63 Riverside Drive, Dundee", "07800444444", "1987-12-11", "DLD456789D"),
        (5, "Ava", "Taylor", "5 Meadow Lane, Stirling", "07800555555", "1998-04-07", "DLE567890E"),
    ],
)

connection.execute(
    """
    INSERT INTO HireAgreement (
        HireNumber,
        StartDate,
        EndDate,
        MileageBefore,
        MileageAfter,
        ClientFirstNameSnapshot,
        ClientLastNameSnapshot,
        ClientAddressSnapshot,
        ClientPhoneSnapshot,
        VehicleModelSnapshot,
        VehicleMakeSnapshot,
        ClientNumber,
        RegistrationNumber
    )
    SELECT ?, ?, ?, ?, ?, c.FirstName, c.LastName, c.HomeAddress, c.PhoneNumber, v.Model, v.Make, c.ClientNumber, v.RegistrationNumber
    FROM Client AS c
    JOIN Vehicle AS v ON v.RegistrationNumber = ?
    WHERE c.ClientNumber = ?;
    """,
    (1001, "2026-01-10", "2026-01-14", 24500, 24980, "GLA-1001", 1),
)

connection.execute(
    """
    INSERT INTO HireAgreement (
        HireNumber,
        StartDate,
        EndDate,
        MileageBefore,
        MileageAfter,
        ClientFirstNameSnapshot,
        ClientLastNameSnapshot,
        ClientAddressSnapshot,
        ClientPhoneSnapshot,
        VehicleModelSnapshot,
        VehicleMakeSnapshot,
        ClientNumber,
        RegistrationNumber
    )
    SELECT ?, ?, ?, ?, ?, c.FirstName, c.LastName, c.HomeAddress, c.PhoneNumber, v.Model, v.Make, c.ClientNumber, v.RegistrationNumber
    FROM Client AS c
    JOIN Vehicle AS v ON v.RegistrationNumber = ?
    WHERE c.ClientNumber = ?;
    """,
    (1002, "2026-02-05", "2026-02-08", 24980, 25310, "GLA-1001", 2),
)

connection.execute(
    """
    INSERT INTO HireAgreement (
        HireNumber,
        StartDate,
        EndDate,
        MileageBefore,
        MileageAfter,
        ClientFirstNameSnapshot,
        ClientLastNameSnapshot,
        ClientAddressSnapshot,
        ClientPhoneSnapshot,
        VehicleModelSnapshot,
        VehicleMakeSnapshot,
        ClientNumber,
        RegistrationNumber
    )
    SELECT ?, ?, ?, ?, ?, c.FirstName, c.LastName, c.HomeAddress, c.PhoneNumber, v.Model, v.Make, c.ClientNumber, v.RegistrationNumber
    FROM Client AS c
    JOIN Vehicle AS v ON v.RegistrationNumber = ?
    WHERE c.ClientNumber = ?;
    """,
    (1003, "2026-03-01", "2026-03-04", 18300, 18610, "EDI-2002", 1),
)

connection.execute(
    """
    INSERT INTO HireAgreement (
        HireNumber,
        StartDate,
        EndDate,
        MileageBefore,
        MileageAfter,
        ClientFirstNameSnapshot,
        ClientLastNameSnapshot,
        ClientAddressSnapshot,
        ClientPhoneSnapshot,
        VehicleModelSnapshot,
        VehicleMakeSnapshot,
        ClientNumber,
        RegistrationNumber
    )
    SELECT ?, ?, ?, ?, ?, c.FirstName, c.LastName, c.HomeAddress, c.PhoneNumber, v.Model, v.Make, c.ClientNumber, v.RegistrationNumber
    FROM Client AS c
    JOIN Vehicle AS v ON v.RegistrationNumber = ?
    WHERE c.ClientNumber = ?;
    """,
    (1004, "2026-03-15", "2026-03-20", 8700, 9100, "GLA-1006", 3),
)

connection.execute(
    """
    INSERT INTO HireAgreement (
        HireNumber,
        StartDate,
        EndDate,
        MileageBefore,
        MileageAfter,
        ClientFirstNameSnapshot,
        ClientLastNameSnapshot,
        ClientAddressSnapshot,
        ClientPhoneSnapshot,
        VehicleModelSnapshot,
        VehicleMakeSnapshot,
        ClientNumber,
        RegistrationNumber
    )
    SELECT ?, ?, ?, ?, ?, c.FirstName, c.LastName, c.HomeAddress, c.PhoneNumber, v.Model, v.Make, c.ClientNumber, v.RegistrationNumber
    FROM Client AS c
    JOIN Vehicle AS v ON v.RegistrationNumber = ?
    WHERE c.ClientNumber = ?;
    """,
    (1005, "2026-04-01", None, 12200, None, "DUN-4004", 4),
)

connection.commit()

queries = [
    (
        "Schema Objects Created",
        """
        SELECT type, name
        FROM sqlite_master
        WHERE type IN ('table', 'trigger') AND name NOT LIKE 'sqlite_%'
        ORDER BY type, name;
        """,
        (),
    ),
    ("Outlet Table Contents", "SELECT * FROM Outlet ORDER BY OutletNumber;", ()),
    ("Staff Table Contents", "SELECT * FROM Staff ORDER BY StaffNumber;", ()),
    ("Vehicle Table Contents", "SELECT * FROM Vehicle ORDER BY RegistrationNumber;", ()),
    ("Client Table Contents", "SELECT * FROM Client ORDER BY ClientNumber;", ()),
    ("HireAgreement Table Contents", "SELECT * FROM HireAgreement ORDER BY HireNumber;", ()),
    (
        "Transaction 1: Vehicles stocked at outlet 1",
        """
        SELECT o.OutletNumber, o.Address, v.RegistrationNumber, v.Make, v.Model,
               v.CurrentMileage, v.DailyHireRate
        FROM Outlet AS o
        JOIN Vehicle AS v ON v.OutletNumber = o.OutletNumber
        WHERE o.OutletNumber = ?
        ORDER BY v.RegistrationNumber;
        """,
        (1,),
    ),
    (
        "Transaction 2: Staff working at outlet 1",
        """
        SELECT o.OutletNumber, s.StaffNumber, s.FirstName, s.LastName,
               s.JobTitle, s.DateJoined, s.Salary
        FROM Outlet AS o
        JOIN Staff AS s ON s.OutletNumber = o.OutletNumber
        WHERE o.OutletNumber = ?
        ORDER BY s.StaffNumber;
        """,
        (1,),
    ),
    (
        "Transaction 3: Vehicles hired by client 1",
        """
        SELECT c.ClientNumber, c.FirstName, c.LastName, h.HireNumber,
               h.StartDate, h.EndDate, v.RegistrationNumber, v.Make, v.Model
        FROM Client AS c
        JOIN HireAgreement AS h ON h.ClientNumber = c.ClientNumber
        JOIN Vehicle AS v ON v.RegistrationNumber = h.RegistrationNumber
        WHERE c.ClientNumber = ?
        ORDER BY h.StartDate;
        """,
        (1,),
    ),
    (
        "Transaction 4: Clients who hired vehicle GLA-1001",
        """
        SELECT v.RegistrationNumber, h.HireNumber, h.StartDate, h.EndDate,
               c.ClientNumber, c.FirstName, c.LastName, c.DrivingLicenseNumber
        FROM Vehicle AS v
        JOIN HireAgreement AS h ON h.RegistrationNumber = v.RegistrationNumber
        JOIN Client AS c ON c.ClientNumber = h.ClientNumber
        WHERE v.RegistrationNumber = ?
        ORDER BY h.StartDate;
        """,
        ("GLA-1001",),
    ),
    (
        "Transaction 5: Details for hire agreement 1003",
        """
        SELECT h.HireNumber, h.StartDate, h.EndDate, h.MileageBefore, h.MileageAfter,
               c.ClientNumber, c.FirstName, c.LastName, c.PhoneNumber,
               v.RegistrationNumber, v.Make, v.Model,
               o.OutletNumber, o.Address, o.PhoneNumber AS OutletPhone
        FROM HireAgreement AS h
        JOIN Client AS c ON c.ClientNumber = h.ClientNumber
        JOIN Vehicle AS v ON v.RegistrationNumber = h.RegistrationNumber
        JOIN Outlet AS o ON o.OutletNumber = v.OutletNumber
        WHERE h.HireNumber = ?;
        """,
        (1003,),
    ),
]

for title, sql, params in queries:
    cursor = connection.execute(sql, params)
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    print(f"\n{title}")
    print(sql.strip())
    if params:
        print(f"Parameters: {params}")
    print(tabulate(rows, headers=columns, tablefmt="grid", missingval="NULL"))

connection.close()
print(f"\nCreated {db_path.name}")
