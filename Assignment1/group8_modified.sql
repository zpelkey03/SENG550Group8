
-- Table cleanup for assignment
DROP TABLE IF EXISTS students CASCADE;

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

SELECT name, email FROM students
