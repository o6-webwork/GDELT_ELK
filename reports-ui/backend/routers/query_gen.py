from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import os, requests
from elasticsearch import Elasticsearch

from data_loader import load_data_from_elasticsearch

ES_INDEX = "gkg"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

LLM_URL = "http://192.168.1.111:80/vllm_qwen2.5/v1/chat/completions"

TOKEN = "token-abc123"

app.get("/query_gen")
def generate_query(user_prompt: str):
    system_prompt = """
        You are an expert at translating plain English questions into queries for the GDELT Global Knowledge Graph (GKG).
        Use the following schema reference for building your queries:

        COLUMNS:
        these columns should usually be included unless stated explicitly or implicity otherwise
        - V2DocId: Link to article
        - V2ExtrasXML.Author: Author of article
        - V2ExtrasXML.Title: Title of article
        - V2ExtrasXML.PubTimestamp: Recorded published time in article
        - V2ExtrasXML.AltUrl: Alternate article links
        - V2ExtrasXML.AltUrlAmp: Amp of alternate article links
        - V2ExtrasXML.Links: Links within article
        - V21Date: Stated published time in article
        - RecordId: ID of record
        - @version: Versions of article

        these columns should be given when user requests for its relevant information
        - log.file.path: Gives path to json file in logstash folder
        - @timestamp: Time which article is added to database
        - GkgRecordId.Date: Time which article is published
        - host.name: String of host name
        - location: coordinates of location
        - V2SrcCmnName.V2SrcCmnName: Webpage of news source where the article came from
        - V2SrcCollectionId.V2SrcCollectionId: Where the article is sourced from
        - V2EnhancedThemes.V2Theme: Contains tags that give a summary of what the article covers in theme tags
        - V21Quotations.Verb: Verbs in quote if quote is present
        - V21Quotations.Quote: List of quotes or string of a quote made in the article
        - V2GCAM.DictionaryDimId: Provides data about the quantity of words used in the article
        - GkgRecordId.Translingual: Bool if document is translingual
        - GkgRecordId.NumberInBatch: Batch number the document was processed in before being added to the database
        - V15Tone.PositiveScore: Score of how postivie the article is
        - V15Tone.NegativeScore: Score of how negative the article is
        - V15Tone.Polarity: Score of how polarising the article is
        - V15Tone.Tone: Score of how postive of negative the article is
        - V15Tone.ActivityRefDensity: percentage of words that were active words offering a very basic proxy of the overall “activeness” of the text compared with a clinically descriptive tex
        - V15Tone.SelfGroupRefDensity: percentage of all words in the article that are pronouns, capturing a combination of self-references and group-based discourse
        - V2Orgs.V1Org: List or string of organisations mentioned in the article
        - V21ShareImg: Links to images in article
        - V21SocVideo.V21SocVideo: Links to videos in article
        - V21AllNames.Name: Names of people,places and concepts mentioned in article
        - V2Locations.FullName: Names of places mentioned in article
        - V2Locations.LocationLatitude: Latitude location of article
        - V2Locations.LocationLongitude: Longitude location of article
        - V2Locations.CountryCode: Country code of article
        - V2Locations.ADM1Code: First set of ADM code of article
        - V2Locations.ADM2Code: Second set of ADM code of article
        - V2Locations.FeatureId: Feature id of article
        - V2Persons.V1Person: Names of people mentioned in article
        - event.original: Information of article when it was added to database

        Here is the structure of the query:
        {
            "_source": [List of columns to be queried for],
            "query": {
                "match_all": {
                }
            }
        }

        Examples:
        ---
        Q: Give me the positive and negative sentiment of article
        A: 
        {You are an expert at translating plain English questions into SQL queries for the GDELT Global Knowledge Graph (GKG).
        Use the following schema reference for building your SQL queries:

        COLUMNS:
        these columns should usually be included unless stated explicitly or implicity otherwise
        - V2DocId: Link to article
        - V2ExtrasXML.Author: Author of article
        - V2ExtrasXML.Title: Title of article
        - V2ExtrasXML.PubTimestamp: Recorded published time in article
        - V2ExtrasXML.AltUrl: Alternate article links
        - V2ExtrasXML.AltUrlAmp: Amp of alternate article links
        - V2ExtrasXML.Links: Links within article
        - V21Date: Stated published time in article
        - RecordId: ID of record
        - @version: Versions of article

        these columns should be given when user requests for its relevant information
        - log.file.path: Gives path to json file in logstash folder
        - @timestamp: Time which article is added to database
        - GkgRecordId.Date: Time which article is published
        - host.name: String of host name
        - location: coordinates of location
        - V2SrcCmnName.V2SrcCmnName: Webpage of news source where the article came from
        - V2SrcCollectionId.V2SrcCollectionId: Where the article is sourced from
        - V2EnhancedThemes.V2Theme: Contains tags that give a summary of what the article covers in theme tags
        - V21Quotations.Verb: Verbs in quote if quote is present
        - V21Quotations.Quote: List of quotes or string of a quote made in the article
        - V2GCAM.DictionaryDimId: Provides data about the quantity of words used in the article
        - GkgRecordId.Translingual: Bool if document is translingual
        - GkgRecordId.NumberInBatch: Batch number the document was processed in before being added to the database
        - V15Tone.PositiveScore: Score of how postive the article is
        - V15Tone.NegativeScore: Score of how negative the article is
        - V15Tone.Polarity: Score of how polarising the article is
        - V15Tone.Tone: Score of how postive of negative the article is
        - V15Tone.ActivityRefDensity: percentage of words that were active words offering a very basic proxy of the overall “activeness” of the text compared with a clinically descriptive tex
        - V15Tone.SelfGroupRefDensity: percentage of all words in the article that are pronouns, capturing a combination of self-references and group-based discourse
        - V2Orgs.V1Org: List or string of organisations mentioned in the article
        - V21ShareImg: Links to images in article
        - V21SocVideo.V21SocVideo: Links to videos in article
        - V21AllNames.Name: Names of people,places and concepts mentioned in article
        - V2Locations.FullName: Names of places mentioned in article
        - V2Locations.LocationLatitude: Latitude location of article
        - V2Locations.LocationLongitude: Longitude location of article
        - V2Locations.CountryCode: Country code of article
        - V2Locations.ADM1Code: First set of ADM code of article
        - V2Locations.ADM2Code: Second set of ADM code of article
        - V2Locations.FeatureId: Feature id of article
        - V2Persons.V1Person: Names of people mentioned in article
        - event.original: Information of article when it was added to database

        Here is the structure of the query:
        {
            "_source": [List of columns to be queried for],
            "query": {
                "match_all": {
                }
            }
        }

        Examples:
        ---
        Q: Give me the positive and negative sentiment of article
        A: 
        {
            "_source": [
                "V2DocId"
                "V2ExtrasXML.Author"
                "V2ExtrasXML.Title"
                "V2ExtrasXML.PubTimestamp"
                "V2ExtrasXML.AltUrl"
                "V2ExtrasXML.AltUrlAmp"
                "V2ExtrasXML.Links"
                "V21Date"
                "RecordId"
                "@version"
            ],
            "query": {
                "match_all": {
                }
            }
        }

        ---
        Q: I want only the themes of the document
            "_source": [
                "V2DocId"
                "V2ExtrasXML.Author"
                "V2ExtrasXML.Title"
                "V2ExtrasXML.PubTimestamp"
                "V2ExtrasXML.AltUrl"
                "V2ExtrasXML.AltUrlAmp"
                "V2ExtrasXML.Links"
                "V21Date"
                "RecordId"
                "@version"
            ],
            "query": {
                "match_all": {
                }
            }
        }

        ---
        Q: I want only the themes of the document
        A: 
        {
            "_source": [
                "V2EnhancedThemes.V2Theme"
            ],
            "query": {
                "match_all": {
                }
            }
        }
        ---
        To handle datetime queries, dates must follow the sequence
        ---
        With this context, think deeply and without adding additional text edit the "_source" parameter to generate only the DELT GKG query based on this user request:<|im_end|>
    """
    r = requests.post(
        LLM_URL,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {TOKEN}"
        },
        json={
            "model": "qwen2.5",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "temperature": 0
        }
    )
    
    return r.json()["choices"][0]["message"]["content"]