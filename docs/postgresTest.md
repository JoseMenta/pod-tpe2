## Test con Postgres

Para poder comparar los resultados obtenidos con Hazelcast, se puede utilizar Postgres con un subconjunto de los datos proporcionados. 
Para eso, se detallan a continuación los pasos para configurar una BD Postgres con un subset de los datos de Chicago.

Se necesitará [Docker](https://www.docker.com/) para levantar un contenedor con la BD, y se supone que se tienen descargados los archivos _infractionsCHI.csv_ y _ticketsCHI.csv_.

### Configuración

1. Crear el contenedor de Docker
```bash 
docker run --name postgres -p 5432:5432 -v my_postgres_data:/var/lib/postgresql/data -e POSTGRES_PASSWORD=postgres -d postgres
```

2. Obtener un subset de los datos
```bash
 head -n 1000001 ticketsCHI.csv > ticketsCHI_reduced.csv
 mv ticketsCHI_reduced.csv ticketsCHI.csv 
```
3. Copiar los archivos en el contenedor
```bash
 docker cp infractionsCHI.csv postgres:.  
 docker cp ticketsCHI.csv postgres:.  
```
4. Crear las tablas en el contenedor
```postgresql 
 CREATE TABLE infractions(
                             violation_code varchar,
                             violation_description varchar
 );

CREATE TABLE tickets(
                        issue_date timestamp,
                        licence_plate_number varchar,
                        violation_code varchar,
                        unit_description varchar,
                        fine_level1_amount integer,
                        community_area_name varchar
);
```
5. Abrir una terminal en el contenedor
```bash
 docker exec -ti postgres bash
```
6. Entrar en la consola de Postgres
```bash
 psql -U postgres 
```
7. Cargar los datos de los archivos
```bash
 COPY tickets(issue_date,licence_plate_number,violation_code,unit_description,fine_level1_amount,community_area_name) FROM '/ticketsCHI.csv' DELIMITER ';' CSV HEADER;
 COPY infractions(violation_code,violation_description) FROM '/infractionsCHI.csv' DELIMITER ';' CSV HEADER; 
```

### Consultas

Se dejan como ejemplo de implementación las siguientes consultas SQL para comparar con la salida esperada

#### Q1
```postgresql
SELECT i.violation_description,count(*)
FROM tickets t JOIN infractions i ON i.violation_code = t.violation_code
GROUP BY i.violation_description
ORDER BY count(*) DESC, i.violation_description;
```

#### Q2
```postgresql
WITH violation_counts AS (
    SELECT
        t.community_area_name,
        t.violation_code,
        i.violation_description,
        COUNT(*) AS violation_count
    FROM
        tickets t
            JOIN
        infractions i ON t.violation_code = i.violation_code
    GROUP BY
        t.community_area_name,
        t.violation_code,
        i.violation_description
    ORDER BY community_area_name, violation_count DESC , violation_code
),
     ranked_violations AS (
         SELECT
             community_area_name,
             violation_code,
             violation_description,
             violation_count,
             ROW_NUMBER() OVER (PARTITION BY community_area_name ORDER BY violation_count DESC) AS rn
         FROM
             violation_counts
     ),
     aux AS (
         SELECT
             community_area_name,
             violation_code,
             violation_description,
             violation_count
         FROM
             ranked_violations
         WHERE
             rn <= 3
     ),
     combined_violations AS (
         SELECT
             community_area_name,
             MAX(CASE WHEN rn = 1 THEN violation_description ELSE NULL END) AS violation_description_1,
             MAX(CASE WHEN rn = 2 THEN violation_description ELSE NULL END) AS violation_description_2,
             MAX(CASE WHEN rn = 3 THEN violation_description ELSE NULL END) AS violation_description_3
         FROM
             (SELECT
                  community_area_name,
                  violation_description,
                  ROW_NUMBER() OVER (PARTITION BY community_area_name ORDER BY violation_count DESC) as rn
              FROM aux) ranked_aux
         GROUP BY
             community_area_name
     )
SELECT DISTINCT
    community_area_name,
    violation_description_1,
    violation_description_2,
    violation_description_3
FROM
    combined_violations
ORDER BY
    community_area_name;
```


#### Q3
```postgresql
WITH total_amount AS(
    SELECT sum(tickets.fine_level1_amount) as total
    FROM tickets
)
SELECT unit_description, concat(round(sum(tickets.fine_level1_amount)/total::numeric * 100,2),'%')
FROM tickets, total_amount
GROUP BY unit_description, total
ORDER BY sum(tickets.fine_level1_amount)/total::numeric DESC;
```

#### Q4
```postgresql
WITH tickets_per_area_and_plate AS(
    SELECT tickets.community_area_name, tickets.licence_plate_number, count(*) as total
    FROM tickets
    WHERE issue_date>='1999-10-08 00:00:00.000000'::timestamp AND issue_date <= '2008-05-31 00:00:00.000000'::timestamp
    GROUP BY tickets.community_area_name, tickets.licence_plate_number
)
SELECT t.community_area_name,max(t.licence_plate_number), t.total
FROM tickets_per_area_and_plate t JOIN (SELECT community_area_name, max(total) as max
                                        FROM tickets_per_area_and_plate
                                        GROUP BY community_area_name) aux ON t.community_area_name = aux.community_area_name
WHERE t.total = aux.max
GROUP BY t.community_area_name,total;
```

#### Q5
```postgresql
WITH aux AS(
SELECT infractions.violation_description, floor((sum(tickets.fine_level1_amount)/count(tickets))::numeric/100::decimal)*100 g
FROM tickets JOIN infractions ON infractions.violation_code = tickets.violation_code
GROUP BY infractions.violation_description
HAVING (sum(tickets.fine_level1_amount)/count(tickets))::numeric >= 100)
SELECT aux.g, aux.violation_description, a2.violation_description
FROM aux, LATERAL(
    SELECT a2.violation_description
    FROM aux a2
    WHERE aux.g = a2.g AND aux.violation_description < a2.violation_description
) a2
ORDER BY g DESC , aux.violation_description, a2.violation_description;
```