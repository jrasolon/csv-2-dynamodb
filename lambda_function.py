import boto3

s3_client = boto3.client("s3")

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('ddb-indexer-elasticsearch-dev-ourcompanies')

def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    s3_file_name = event['Records'][0]['s3']['object']['key']
    resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_name)
    data = resp['Body'].read().decode("utf-8")
    companies = data.split("\n")
    for comp in companies: 
        print(comp)
        comp_data = comp.split(",")
        #Add it to dynamodb 
        table.put_item(
            Item = {
                "siren": comp_data[0],
                "nic": comp_data[1],
                "siret": comp_data[2],
                "statutDiffusionEtablissement": comp_data[3],
                "dateCreationEtablissement": comp_data[4],
                "trancheEffectifsEtablissement": comp_data[5],
                "anneeEffectifsEtablissement": comp_data[6],
                "activitePrincipaleRegistreMetiersEtablissement": comp_data[7],
                "dateDernierTraitementEtablissement": comp_data[8],
                "etablissementSiege": comp_data[9],
                "nombrePeriodesEtablissement": comp_data[10],
                "complementAdresseEtablissement": comp_data[11],
                "numeroVoieEtablissement": comp_data[12],
                "indiceRepetitionEtablissement": comp_data[13],
                "typeVoieEtablissement": comp_data[14],
                "libelleVoieEtablissement": comp_data[15],
                "codePostalEtablissement": comp_data[16],
                "libelleCommuneEtablissement": comp_data[17],
                "libelleCommuneEtrangerEtablissement": comp_data[18],
                "distributionSpecialeEtablissement": comp_data[19],
                "codeCommuneEtablissement": comp_data[20],
                "codeCedexEtablissement": comp_data[21],
                "libelleCedexEtablissement": comp_data[22],
                "codePaysEtrangerEtablissement": comp_data[23],
                "libellePaysEtrangerEtablissement": comp_data[24],
                "complementAdresse2Etablissement": comp_data[25],
                "numeroVoie2Etablissement": comp_data[26],
                "indiceRepetition2Etablissement": comp_data[27],
                "typeVoie2Etablissement": comp_data[28],
                "libelleVoie2Etablissement": comp_data[29],
                "codePostal2Etablissement": comp_data[30],
                "libelleCommune2Etablissement": comp_data[31],
                "libelleCommuneEtranger2Etablissement": comp_data[32],
                "distributionSpeciale2Etablissement": comp_data[33],
                "codeCommune2Etablissement": comp_data[34],
                "codeCedex2Etablissement": comp_data[35],
                "libelleCedex2Etablissement": comp_data[36],
                "codePaysEtranger2Etablissement": comp_data[37],
                "libellePaysEtranger2Etablissement": comp_data[38],
                "dateDebut": comp_data[39],
                "etatAdministratifEtablissement": comp_data[40],
                "enseigne1Etablissement": comp_data[41],
                "enseigne2Etablissement": comp_data[42],
                "enseigne3Etablissement": comp_data[43],
                "denominationUsuelleEtablissement": comp_data[44],
                "activitePrincipaleEtablissement": comp_data[45],
                "nomenclatureActivitePrincipaleEtablissement": comp_data[46],
                "caractereEmployeurEtablissement": comp_data[47],

            }
        )
