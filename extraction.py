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
    MISTRAL_API_KEY = 'xxx' # CLE MISTRAL ICI

client = Mistral(api_key=MISTRAL_API_KEY)

"""Indiquer ici l'URL du Vatican à scrapper"""

BASE_URL = "https://www.vatican.va/archive/FRA0013/"
INDEX_URL = urljoin(BASE_URL, "_INDEX.HTM")

"""Noms des dossiers S3 où vont être sauvegardés les résultats"""

OUTPUT_PATH = "./dataset/vatican_pipeline"
OUTPUT_DIR_S1 = "pages_markdown"
OUTPUT_DIR_S2 = "groupes_markdown"
INDEX_JSON = "pages_index.json"

"""Liste noir des pages/liens à ne pas scrapper"""
BLACKLIST = ["AIDE", "INDEX", "TABLE", "SOMMAIRE", "INTRODUCTION", "LIENS UTILES", "EN GENERAL", "LISTE DES SIGLES"]


def get_soup(url):
    """Télécharge une page et retourne le BeautifulSoup"""
    response = requests.get(url)
    response.encoding = 'utf-8'
    return BeautifulSoup(response.text, 'html.parser')


def get_liste_liens(index_url):
    """Récupère les liens du sommaire de la page"""
    soup = get_soup(index_url)
    links = soup.select("a[href]")
    pages = []

    for link in links:
        href = link.get("href")
        text = link.get_text(strip=True)

        if len(text) == 0:
            continue
        
        if any(badword in unidecode(text.upper()) for badword in BLACKLIST):
            continue
            
        if href.endswith(".HTM") and not href.startswith("mailto:"):
            full_url = urljoin(BASE_URL, href)
            pages.append((text, full_url))
    return pages

def extraire_texte_llm(soup: BeautifulSoup, model="mistral-large-latest") -> str:
    """Envoie le HTML au LLM avec une instruction de transformation en Markdown"""
    body = soup.find("body")
    if not body:
        return ""

    contenu_html = str(body)
    prompt = (
        "Tu es un assistant qui convertit le contenu HTML en Markdown structuré et lisible.\n"
        "Garde les titres, paragraphes, citations, numérotations, et ignore les éléments de navigation.\n"
        "Voici le HTML à traiter :\n\n"
        f"{contenu_html}\n\n"
        "Réponds uniquement avec le contenu converti en Markdown."
    )
    alt_messages = [{"role": "user", "content": prompt}]
    chat_response = client.chat.complete(model=model, messages=alt_messages, temperature=0, random_seed=0)
        
    return chat_response.choices[0].message.content.strip()


def nettoyer_nom(titre):
    """Crée un nom de fichier sûr à partir du titre"""
    titre = re.sub(r"[^\w\s-]", "", titre)
    return unidecode(titre.strip().replace(" ", "_")[:60])

    
def enregistrer_markdown(titre, texte_markdown, index):
    """Sauvegarde un fichier markdown dans le S3"""
    clean_title = f"{index:03d}_{titre}.md"
    filepath = os.path.join(OUTPUT_PATH, OUTPUT_DIR_S1, clean_title)
    if is_on_allonia_platform:
        path = S3Path(filepath)
        path.write(texte_markdown)
    else:
        with open(filepath, "w", encoding="utf-8") as f:
                f.write(texte_markdown)

    print(f"✔️ Sauvegardé : {filepath}")
    return clean_title


def main():
    pages = get_liste_liens(INDEX_URL)    
    index_data = []
    
    for i, (titre, url) in enumerate(pages, 1):
        print(f"🔹 {i:03d} - {titre} → {url}")
        try:
            soup_page = get_soup(url)
            #nettoyer_html(soup_page)
            markdown = extraire_texte_llm(soup_page)
            clean_titre = nettoyer_nom(titre)
            nom_fichier = enregistrer_markdown(clean_titre, markdown, i + 1)

            index_data.append({
                "titre": clean_titre,
                "url": url,
                "fichier": nom_fichier
            })

            time.sleep(2)

        except Exception as e:
            print(f"⚠️ Erreur sur {titre} : {e}")

    if is_on_allonia_platform:
        filepath = os.path.join(OUTPUT_PATH, INDEX_JSON)
        path = S3Path(filepath)
        index_data_dict = {index: value for index, value in enumerate(index_data)}
        path.write(index_data_dict)

    else:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(index_data, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Extraction terminée : {len(index_data)} pages extraites.")
    print(f"📄 Index écrit dans {INDEX_JSON}")

if __name__ == "__main__":
    main()
