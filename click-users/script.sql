CREATE USER jhon IDENTIFIED WITH plaintext_password BY 'qwerty';

CREATE ROLE devs;

GRANT SELECT ON *.* TO devs;

GRANT devs TO jhon;