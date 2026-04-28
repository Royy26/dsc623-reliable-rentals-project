# Reliable Rentals Database Project

This repository contains the full three-part DSC623 database project for the fictional car rental company Reliable Rentals.

The project develops the database from conceptual design to logical design and finally to physical implementation using Python and SQLite.

## Project Overview

The project is organised into three main stages.

Part 1 develops the conceptual design of the database, including the entity-relationship model, assumptions, entities, attributes, and relationship multiplicities.

Part 2 translates the conceptual design into a logical relational schema, validates the design through normalization to 3NF, and defines the five required user transaction queries.

Part 3 implements the database physically using embedded SQL in Python with SQLite, inserts sample data, and executes the required queries.

## Repository Structure

- `submission/part1/` contains the final Part 1 report and conceptual diagram files prepared for submission.
- `submission/part2/` contains the final Part 2 report and logical diagram files prepared for submission.
- `submission/part3/` contains the final Part 3 Python implementation and report prepared for submission.

## Part 3 Implementation

The final database implementation provided in this repository is `submission/part3/Part3.py`.

This script uses embedded SQL with Python and SQLite to:

1. create the full database schema,
2. apply primary key, foreign key, check, and trigger constraints,
3. insert sample data into the main business relations,
4. display the contents of the database tables, and
5. execute the five user transaction queries required by Part 2.C.

When the script is run, it generates the SQLite database file `reliable_rentals_part3.db` in the same folder as the script.

## Main Database Relations

The implementation is based on the following main business relations:

- `Outlet`
- `Staff`
- `Vehicle`
- `Client`
- `HireAgreement`

The physical implementation also includes an auxiliary support relation named `OutletStaffCoverage` to enforce the rule that every outlet must always retain at least one staff member.

## Technologies Used

- Python 3
- SQLite 3
- Embedded SQL
- `tabulate` for formatted console output

## How to Run Part 3

Move into the Part 3 folder and run the Python implementation script.

```bash
cd submission/part3
pip install tabulate
python Part3.py
```

Running the script will:

1. delete any previous copy of the SQLite database file,
2. create a new database from scratch,
3. build the schema and constraints,
4. insert sample data,
5. print the database contents and query results, and
6. create `reliable_rentals_part3.db` in the `submission/part3/` folder.

## Reports and Supporting Materials

This repository is intended to include the documentation generated across all three project parts.

These materials include the final conceptual design report, logical design report, implementation code, diagrams, and the final Part 3 report that references the project GitHub repository.

## Notes

The final Part 3 implementation uses Python as the host language and SQLite as the database engine, which satisfies the assignment requirement to implement the database using embedded SQL.

The SQL code is not separated into a standalone `.sql` file because the assignment explicitly allows the use of Python and SQLite for the implementation.

Instead, the SQL statements are embedded directly inside the Python program and executed through the SQLite interface.
