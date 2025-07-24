is_on_allonia_platform = False

import os, re, time, requests, json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from mistralai import Mistral
from unidecode import unidecode

if is_on_allonia_platform:
    from allonias3 import S3Path
    from alloniarest.external_api_keys import get_external_api_key_value
    MISTRAL_API_KEY = get_external_api_key_value("ALLONIA_MISTRAL") 
else:
    MISTRAL_API_KEY = 'xxxx' # CLE MISTRAL ICI

client = Mistral(api_key=MISTRAL_API_KEY)


"""Noms des dossiers S3 où vont être sauvegardés les résultats"""

OUTPUT_PATH = "./dataset/vatican_pipeline"
OUTPUT_DIR_S1 = "pages_markdown"
OUTPUT_DIR_S2 = "groupes_markdown"
INDEX_JSON = "pages_index.json"

REGROUPEMENT_CATECHISME = {
    "type": "titre_prefixe",
    "grandes_parties": [
        "PROLOGUE",
        'PREMIERE SECTION "JE CROIS" – "NOUS CROYONS"',
        "DEUXIEME PARTIE LA CELEBRATION DU MYSTERE CHRETIEN",
        "TROISIEME PARTIE LA VIE DANS LE CHRIST",
        "PREMIERE SECTION LA PRIERE DANS LA VIE CHRETIENNE"
    ]
}

def load_file(file_path):
    path = S3Path(file_path)
    return path.read()

def normaliser_nom(titre):
    """ Transforme un titre en un nom de fichier propre """
    titre = titre.lower().strip().replace(" ", "_")
    titre = "".join(c for c in titre if c.isalnum() or c == "_")
    return unidecode(titre[:50])

def detecter_partie_par_prefixe(titre, grandes_parties):
    """Retourne la partie correspondant à un titre selon les grands préfixes"""
    titre_upper = titre.upper()
    for partie in grandes_parties:
        if titre_upper.startswith(partie.upper()):
            return partie.title()
    return None


def regrouper_par_titre_prefixe(index, grandes_parties):
    """ Parcourt l'index et regroupe les fichiers Markdown par grandes parties. """
    
    groupes = {partie.title(): [] for partie in grandes_parties}
    partie_courante = None

    for item in index:
        titre = item["titre"]
        fichier = item["fichier"]
        partie_detectee = detecter_partie_par_prefixe(titre, grandes_parties)
        if partie_detectee:
            partie_courante = partie_detectee
        if partie_courante:
            groupes[partie_courante].append(fichier)

    return [{"nom": nom, "fichiers": fichiers} for nom, fichiers in groupes.items() if fichiers]

def regrouper_pages(index, schema):
    if schema["type"] == "titre_prefixe":
        return regrouper_par_titre_prefixe(index, schema["grandes_parties"])
    else:
        raise ValueError(f"Type de regroupement inconnu : {schema['type']}")

def assembler_groupes(groupes): # (dossier_markdown, dossier_sortie)
    """
    Pour chaque groupe, lit les fichiers Markdown individuels,
    les concatène, et écrit un seul fichier Markdown par groupe.
    """
    
    for i, groupe in enumerate(groupes, 1):
        contenu_total = ""
        for fichier in groupe["fichiers"]:
            path = os.path.join(OUTPUT_PATH, OUTPUT_DIR_S1, fichier)
            contenu = load_file(path)
            contenu_total += f"\n\n---\n\n{contenu}"

        nom_fichier = f"{i:02d}_{normaliser_nom(groupe['nom'])}.md"
        chemin = os.path.join(OUTPUT_PATH, OUTPUT_DIR_S2, nom_fichier)
        path = S3Path(chemin)
        path.write(contenu_total.strip())

        print(f"✅ Groupe : {groupe['nom']} → {nom_fichier}")
        print(f"✔️ Sauvegardé : {chemin}")


def main():
    index_json_path = os.path.join(OUTPUT_PATH, INDEX_JSON)
    index = load_file(index_json_path)
    groupes = regrouper_pages(list(index.values()), REGROUPEMENT_CATECHISME)
    print(f"{len(groupes)} groupes détectés : {[g['nom'] for g in groupes]}")
    assembler_groupes(groupes) #("pages_markdown", "catéchisme_regroupé")
    print("\n✅ Tous les groupes ont été assemblés.")

if __name__ == "__main__":
    main()
