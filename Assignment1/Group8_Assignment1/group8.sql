-- Table cleanup for assignment
DROP TABLE IF EXISTS students CASCADE;
DROP TABLE IF EXISTS products CASCADE;

-- Question 1 - 
CREATE TABLE students (
	id 		SERIAL 	PRIMARY KEY,
	name 	TEXT 	NOT NULL,
	age		INT		NOT NULL,
	major 	TEXT	NOT NULL
);

INSERT INTO students (name, age, major) VALUES
('John', 19, 'Arts'),
('Mark', 27, 'Computer Science'),
('Luke', 21, 'Engineering'),
('Matthew', 18, 'Engineering');

--Question 1 Queries
SELECT * FROM students;

SELECT * FROM students WHERE age > 21;

SELECT name FROM students WHERE major = 'Computer Science';

--Question 2
ALTER TABLE students
ADD COLUMN email VARCHAR(100);

UPDATE students
SET email = 'john@icloud.com'
WHERE name = 'John';

UPDATE students
SET email = 'mark@icloud.com'
WHERE name = 'Mark';

SELECT name, email FROM students;

--Question 3
SELECT * FROM STUDENTS;

SELECT COUNT('*') FROM STUDENTS;

-- Question 4
CREATE TABLE products(
	product_id 	int UNIQUE,
	name 	TEXT 	NOT NULL,
	price	FLOAT   NOT NULL
);

-- After importing products.csv through PgAdmin

SELECT * FROM products;

SELECT * FROM products WHERE price < 2.00;

SELECT MAX(price) as MaximumPrice FROM products;

--Question 5
DELETE FROM students WHERE name = 'John';

SELECT * FROM students ORDER BY AGE DESC;

SELECT * 
FROM students
WHERE age  = (SELECT(MIN(age)) FROM students);

--Question 6
SELECT major, COUNT ('*') FROM students GROUP BY major;

SELECT AVG(price) as averageprice from products;

SELECT SUM(price) as Total from products;

-- Question 7 - 
DROP TABLE IF EXISTS enrollments CASCADE;
CREATE TABLE enrollments (
	enroll_id 	SERIAL 	PRIMARY KEY,
	student_id	INT REFERENCES students(id),
	course		TEXT	NOT NULL
);

INSERT INTO enrollments (student_id, course)
SELECT id, 'CPSC 453' FROM students WHERE name = 'Mark';

INSERT INTO enrollments (student_id, course)
SELECT id, 'SENG 550' FROM students WHERE name = 'Luke';

INSERT INTO enrollments (student_id, course)
SELECT id, 'SENG 550' FROM students WHERE name = 'Matthew';

INSERT INTO enrollments (student_id, course)
SELECT id, 'ENSF 460' FROM students WHERE name = 'Matthew';

-- Question 7 Queries

SELECT students.name, enrollments.course
FROM enrollments
JOIN students ON students.id = enrollments.student_id
ORDER BY students.name, enrollments.course;

SELECT students.name
FROM enrollments
JOIN students on students.id = enrollments.student_id
GROUP BY students.name
HAVING COUNT(*) > 1

-- Question 8
INSERT INTO students (name, age, major)
VALUES ('Nicola Savino', 22, 'Computer Science');

INSERT INTO products (product_id, name, price)
VALUES (9001, 'Monitor', 150);

CREATE VIEW "cs_students" AS
SELECT *
FROM students
WHERE major = 'Computer Science';

SELECT * FROM "cs_students";
