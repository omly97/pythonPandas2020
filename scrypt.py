import os
import sqlite3
import pandas


# objet DataFrame du fichier vendor/DENSITE-01.csv
dataFrame = pandas.read_csv("./vendor/DENSITE-01.csv", delimiter=";", dtype={'SUPERFICIE':object, 'POP.84':object, 'POP.88':object, 'POP.86':object})

# creation champs DENSITE.84 DENSITE.88 DENSITE.86 dans le dataFrame
dataFrame['DENSITE.84'] = dataFrame['POP.84'].astype('int64') / dataFrame['SUPERFICIE'].astype('int64')
dataFrame['DENSITE.88'] = dataFrame['POP.88'].astype('int64') / dataFrame['SUPERFICIE'].astype('int64')
dataFrame['DENSITE.86'] = dataFrame['POP.86'].astype('int64') / dataFrame['SUPERFICIE'].astype('int64')

# conversion du dataFrame en un fichier .csv -> vendor/DENSITE-02.csv
dataFrame.to_csv("./vendor/DENSITE-02.csv", sep=";", index=False)


# ouverture de connexion sqlite
connexion = sqlite3.connect("./database/database.db")

# initialisation du curseur
curseur = connexion.cursor()

# creation de la table des nations
curseur.executescript("""
    DROP TABLE IF EXISTS nations;

    CREATE TABLE nations (
        pays TEXT PRIMARY KEY,
        region TEXT NOT NULL,
        superficie FLOAT NOT NULL
    );
""")

# creation de la table des populations des nations
curseur.executescript("""
    DROP TABLE IF EXISTS nation_populations;

    CREATE TABLE nation_populations (
        id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
        pays TEXT,
        annee INTEGER NOT NULL,
        population INTEGER NOT NULL,
        densite FLOAT NOT NULL,
        FOREIGN KEY (pays) REFERENCES nations(pays)
    );
""")

# insertion des nations
nations = dataFrame[['PAYS', 'REGION', 'SUPERFICIE']]
tuples_nations = list(nations.to_records(index=False))
curseur.executemany(""" INSERT INTO nations(pays, region, superficie) VALUES (?,?,?) """, tuples_nations)

# insertion des donnees population de 1984
pop84 = dataFrame[['PAYS', 'POP.84', 'DENSITE.84']]
tuples_pop84 = list(pop84.to_records(index=False))
curseur.executemany(""" INSERT INTO nation_populations(pays, annee, population, densite) VALUES (?,'1984',?,?) """, tuples_pop84)

# insertion des donnees population de 1986
pop86 = dataFrame[['PAYS', 'POP.86', 'DENSITE.86']]
tuples_pop86 = list(pop86.to_records(index=False))
curseur.executemany(""" INSERT INTO nation_populations(pays, annee, population, densite) VALUES (?,'1986',?,?) """, tuples_pop86)

# insertion des donnees population de 1988
pop88 = dataFrame[['PAYS', 'POP.88', 'DENSITE.88']]
tuples_pop88 = list(pop88.to_records(index=False))
curseur.executemany(""" INSERT INTO nation_populations(pays, annee, population, densite) VALUES (?,'1988',?,?) """, tuples_pop88)

# commit
connexion.commit()

# fermeture de la connexion sqlite
connexion.close()
