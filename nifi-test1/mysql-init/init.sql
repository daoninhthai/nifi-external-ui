CREATE TABLE users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  name VARCHAR(50),
  email VARCHAR(50)
);

INSERT INTO users (name, email)
VALUES ('Thai Dao', 'thai@example.com'), ('Lan', 'lan@example.com');
