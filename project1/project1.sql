#DBMS_HW1 2017030273 Jeong Hee Seok

USE Pokemon;

#1
SELECT T.name
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY C.owner_id
HAVING COUNT(*) >= 3
ORDER BY COUNT(*) DESC;

#2
SELECT P.name
FROM Pokemon P,
    (
        SELECT P.type
        FROM Pokemon P
        GROUP BY P.type
        ORDER BY COUNT(*) DESC, P.type LIMIT 2
    ) AS PTYPE
WHERE P.type = PTYPE.type
ORDER BY P.name;
    
#3
SELECT P.name
FROM Pokemon P
WHERE P.name LIKE '_o%'
ORDER BY P.name;

#4
SELECT C.nickname
FROM CatchedPokemon C
WHERE C.level >= 50
ORDER BY C.nickname;

#5
SELECT P.name
FROM Pokemon P
WHERE CHAR_LENGTH(P.name) = 6
ORDER BY P.name;

#6
SELECT T.name
FROM Trainer T
WHERE T.hometown = 'Blue City'
ORDER BY T.name;

#7
SELECT DISTINCT T.hometown
FROM Trainer T
ORDER BY T.hometown;

#8
SELECT AVG(RP.level) AS 'Average Level'
FROM
(
    SELECT C.pid, C.level
    FROM CatchedPokemon C, Trainer T
    WHERE C.owner_id = T.id AND T.name = 'Red'
) AS RP;

#9
SELECT C.nickname
FROM CatchedPokemon C
WHERE C.nickname NOT LIKE 'T%'
ORDER BY C.nickname;

#10
SELECT C.nickname
FROM CatchedPokemon C
WHERE C.level >= 50 AND C.owner_id >= 6
ORDER BY C.nickname;

#11
SELECT P.id, P.name
FROM Pokemon P
ORDER BY P.id;

#12
SELECT C.nickname
FROM CatchedPokemon C
WHERE C.level <= 50
ORDER BY C.level;

#13
SELECT P.name, C.pid
FROM CatchedPokemon C, Pokemon P, Trainer T
WHERE C.owner_id = T.id AND C.pid = P.id AND T.hometown = 'Sangnok City'
ORDER BY C.pid;

#14
SELECT C.nickname
FROM CatchedPokemon C, Gym G, Pokemon P, Trainer T
WHERE G.leader_id = T.id AND C.owner_id = T.id and C.pid = P.id and P.type = 'Water'
ORDER BY C.nickname;

#15
SELECT COUNT(*)
FROM CatchedPokemon C
WHERE C.pid IN (
    SELECT E.before_id
    FROM Evolution E
);

#16
SELECT COUNT(*)
FROM Pokemon P
WHERE P.type = 'Water' OR
      P.type = 'Electric' OR
      P.type = 'Psychic';

#17
SELECT COUNT(*)
FROM
(
    SELECT P.id
    FROM CatchedPokemon CP, Trainer T, Pokemon P
    WHERE T.id = CP.owner_id AND CP.pid = P.id AND T.hometown = 'Sangnok City'
    GROUP BY P.id
) AS SP;

#18
SELECT C.level
FROM CatchedPokemon C, Trainer T
WHERE T.id = C.owner_id AND T.hometown = 'Sangnok City'
ORDER BY C.level DESC LIMIT 1;

#19
SELECT COUNT(*)
FROM Gym G,
    (
        SELECT DISTINCT T.id, P.type
        FROM Trainer T, CatchedPokemon C, Pokemon P
        WHERE T.id = C.owner_id AND C.pid = P.id AND T.hometown = 'Sangnok City'
    ) AS S
WHERE G.leader_id = S.id;

#20
SELECT T.name, S.catched_number
FROM Trainer T,
    (
        SELECT C.owner_id, COUNT(*) AS catched_number
        FROM Trainer T, CatchedPokemon C
        WHERE T.id = C.owner_id AND T.hometown = 'Sangnok City'
        GROUP BY C.owner_id
    ) AS S
WHERE T.id = S.owner_id
ORDER BY S.catched_number;

#21
SELECT P.name
FROM Pokemon P
WHERE (P.name LIKE 'a%' OR P.name LIKE 'A%' OR
       P.name LIKE 'e%' OR P.name LIKE 'E%' OR
       P.name LIKE 'i%' OR P.name LIKE 'I%' OR
       P.name LIKE 'o%' OR P.name LIKE 'O%' OR
       P.name LIKE 'u%' OR P.name LIKE 'U%');

#22
SELECT P.type, COUNT(P.type)
FROM Pokemon P
GROUP BY P.type
ORDER BY COUNT(P.type), P.type;

#23
SELECT DISTINCT T.name
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id AND C.level <= 10
ORDER BY T.name;

#24
SELECT C.name, AVG(CP.level)
FROM City C, Trainer T, CatchedPokemon CP
WHERE C.name = T.hometown AND T.id = CP.owner_id
GROUP BY C.name
ORDER BY AVG(CP.level);

#25
SELECT DISTINCT P.name
FROM Pokemon P,
    (
        SELECT C.pid
        FROM CatchedPokemon C, Trainer T
        WHERE C.owner_id = T.id AND T.hometown = 'Sangnok City'
    ) AS SANGNOK,
    (
        SELECT C.pid
        FROM CatchedPokemon C, Trainer T
        WHERE C.owner_id = T.id AND T.hometown = 'Brown City'
    ) AS BROWN
WHERE P.id = SANGNOK.pid AND SANGNOK.pid = BROWN.pid
ORDER BY P.name;

#26
SELECT P.name
FROM Pokemon P, CatchedPokemon C
WHERE P.id = C.pid AND C.nickname LIKE '% %'
ORDER BY P.name DESC;

#27
SELECT 4_OR_MORE_POKEMON.name, 4_OR_MORE_POKEMON.max_level AS 'MAX Level'
FROM 
    (
         SELECT T.name, MAX(C.level) AS max_level
         FROM Trainer T, CatchedPokemon C
         WHERE T.id = C.owner_id
         GROUP BY T.name
         HAVING count(*) >= 4
    ) AS 4_OR_MORE_POKEMON
ORDER BY 4_OR_MORE_POKEMON.name;

#28
SELECT T.name, AVG(C.level)
FROM Trainer T, CatchedPokemon C, Pokemon P
WHERE T.id = C.owner_id AND C.pid = P.id AND 
     (P.type = 'Normal' OR P.type = 'Electric')
GROUP BY T.name
ORDER BY AVG(C.level);

#29
SELECT P.name AS 'Pokemon name', T.name AS 'Trainer name', C.description
FROM CatchedPokemon CP, Trainer T, City C, Pokemon P
WHERE CP.owner_id = T.id AND CP.pid = P.id AND T.hometown = C.name AND P.id = 152
ORDER BY CP.level DESC;

#30
SELECT DISTINCT P1.id, P1.name AS 'Level1',
                P2.name AS 'Level2',
                P3.name AS 'Level3'
FROM Pokemon P1, Pokemon P2, Pokemon P3,
    (
        SELECT EL.before_id AS 'Level1',
               ER.before_id AS 'Level2',
               ER.after_id AS 'Level3'
        FROM Evolution EL, Evolution ER
        WHERE EL.after_id = ER.before_id
    ) AS EVL
WHERE P1.id = EVL.Level1 AND
      P2.id = EVL.Level2 AND
      P3.id = EVL.Level3    
ORDER BY P1.id;

#31
SELECT P.name
FROM Pokemon P
WHERE P.id >= 10 AND P.id <= 99
ORDER BY P.name;

#32
SELECT P.name
FROM Pokemon P
WHERE P.id NOT IN 
(
    SELECT C.pid
    FROM CatchedPokemon C
)
ORDER BY P.name;

#33
SELECT SUM(C.level) AS 'Sum of Level'
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id AND T.name = 'Matis';

#34
SELECT T.name
FROM Trainer T, Gym G
WHERE T.id = G.leader_id
ORDER BY T.name;

#35
SELECT P.type, (COUNT(*) / (SELECT COUNT(*) FROM Pokemon) * 100) AS 'Percentage'
FROM Pokemon P
GROUP BY P.type
ORDER BY COUNT(*) DESC LIMIT 1;

#36
SELECT T.name
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id AND C.nickname LIKE 'A%'
ORDER BY T.name;

#37
SELECT T.name, SUM(C.level)
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY T.name
ORDER BY SUM(C.level) DESC LIMIT 1;

#38
SELECT P.name
FROM Pokemon P, Evolution E
WHERE P.id = E.after_id AND E.after_id NOT IN 
(
    SELECT E2.after_id
    FROM Evolution E1, Evolution E2
    WHERE E1.after_id = E2.before_id
)
ORDER BY P.name;

#39
SELECT T.name
FROM Trainer T, CatchedPokemon C
WHERE T.id = C.owner_id
GROUP BY C.owner_id, C.pid
HAVING COUNT(*) > 1
ORDER BY T.name;

#40
SELECT CITY.name AS 'City Name', CatchedPokemon.nickname
FROM Trainer T, CatchedPokemon,
    (
        SELECT C.name, MAX(CP.level) AS max_level
        FROM Trainer T, City C, CatchedPokemon CP
        WHERE T.id = CP.owner_id AND T.hometown = C.name
        GROUP BY C.name
    ) AS CITY
WHERE T.hometown = CITY.name AND CatchedPokemon.level = CITY.max_level AND
      T.id = CatchedPokemon.owner_id
ORDER BY CITY.name;