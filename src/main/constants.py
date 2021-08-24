dict_prov_ca = {
    "ALBERTA" : "AB",
    "BRITISH COLUMBIA" : "BC",
    "MANITOBA" : "MB",
    "NEW BRUNSWICK" : "NB",
    "NEWFOUNDLAND AND LABRADOR" : "NL",
    "NORTHWEST TERRITORIES" : "NT",
    "NOVA SCOTIA" : "NS",
    "NUNAVUT" : "NU",
    "ONTARIO" : "ON",
    "PRINCE EDWARD ISLAND" : "PE",
    "QUEBEC" : "QC",
    "SASKATCHEWAN" : "SK",
    "YUKON TERRITORY" : "YT",
    "YUKON" : "YT",
    "LABRADOR" : "NL",
    "NEWFOUNDLAND" : "NL",
    # Common other abbrs to province names
    "MAN" : "MB",
    "ALTA" : "AB",
    "NFLD" : "NL",
    "NF" : "NL",
    "NWT" : "NT",
    "NUN" : "NU",
    "ONT" : "ON",
    "PEI" : "PE",
    "QUE" : "QC",
    "SASK" : "SK",
    "YUK" : "YT",
    "PQ" : "QC",
    "B\.C\." : "BC",
    "B\. C\.": "BC",
    "B\.C": "BC",
    "NS\." : "NS",
    "N\.S\.": "NS",
    "N\.B\.": "NB",
    "P\.E\.I\.": "PE",
    "QU\?BC": "QC",
    "MANIT0BA": "MB",
    "NOVA SCONA":"NS",
    "B C": "BC",
    "N B": "NB",
    # misspellings
    "OUEBEC": "QC"}

strict_regex_fax_extract = r"(?i)[F][A?]{0,}[X?]{0,}[ :.,]{0,}([({\[?\s]{0,}[\d?]{2,3}[-\.\s\)}\]]{0,}[\d?]{3}[-\.\s]{0,}[\d?]{3,4})"
strict_regex_fax_replace = r"(?i)([F][A?]{0,}[X?]{0,}[ :.,]{0,}[({\[?\s]{0,}[\d?]{2,3}[-\.\s\)}\]]{0,}[\d?]{3}[-\.\s]{0,}[\d?]{3,4})"
strict_regex_phone_extract = r"(?i)[T?]{0,}[E?]{0,}[L?]{0,}[P?]{0,}[H?]{0,}[O?]{0,}[N?]{0,}[E?]{0,}[ :.,]{0,}([+]{0,1}[1]{0,1}[-({\[?\s]{0,}[\d?]{2,3}[-\.\s\)}\]]{0,}[\d?]{3}[-\.\s]{0,}[\d?]{3,4})"
strict_regex_phone_replace = r"(?i)([T?]{0,}[E?]{0,}[L?]{0,}[P?]{0,}[H?]{0,}[O?]{0,}[N?]{0,}[E?]{0,}[ :.,]{0,}[+]{0,1}[1]{0,1}[-({\[?\s]{0,}[\d?]{2,3}[-\.\s\)}\]]{0,}[\d?]{3}[-\.\s]{0,}[\d?]{3,4})"

regex_fax_extract = r"(?i)[F][A?]{0,}[X?]{0,}[ :.,]{0,}([-.?(){}\[\]\d\s]{6,})"
regex_fax_replace = r"(?i)([F][A?]{0,}[X?]{0,}[ :.,]{0,}[-.?(){}\[\]\d\s]{6,})"
regex_phone_extract = r"(?i)(?:\bT\b|\bTEL\b|\bTELEPHONE\b|\bPHONE\b|\bPH\b|\bTELL\b){0,}[ :.,#]{0,}([-.?(){}\[\]\d\s]{6,})"
regex_phone_replace = r"(?i)((?:\bT\b|\bTEL\b|\bTELEPHONE\b|\bPHONE\b|\bPH\b|\bTELL\b){0,}[ :.,#]{0,}[-.?(){}\[\]\d\s]{6,})"

regex_email_extract = r"(?i)\b[E?]{0,1}[\s:]{0,}[-]?[\s]{0,}[M?]{0,1}[A?]{0,1}[I?]{0,1}[L?]{0,1}\b[:\s]{0,}([a-zA-Z0-9?'+._-]+[\s]{0,}@[a-zA-Z0-9?'\s_-]+[.][\s]{0,}[a-zA-Z0-9?'._-]+)"
regex_email_replace = r"(?i)(\b[E?]{0,1}[\s:]{0,}[-]?[\s]{0,}[M?]{0,1}[A?]{0,1}[I?]{0,1}[L?]{0,1}\b[:\s]{0,}[a-zA-Z0-9?'+._-]+[\s]{0,}@[a-zA-Z0-9?'\s_-]+[.][\s]{0,}[a-zA-Z0-9?'._-]+)"

regex_url_extract = r"(?i)(?:\bWEBSITE\b[:\s]{0,}|\bWEB\b[\s]{0,}[\\bSITE\\b]{0,}[:\s]{0,}){0,}((?:https?://[w? ]{1,}[\.\s]{1,}|[w?]{1,}[\.]{1,})(?:[-A-Z0-9+&@#\/%=~_|$? !:,.]+\bcom\b|[-A-Z0-9+&@#\/%=~_|$?!: ,.]+\.|[-A-Z0-9+&@#\/%=~_|$?!:,.]+[\.]{0,})[^\s,;]{0,})"
regex_url_replace = r"(?i)((?:\bWEBSITE\b[:\s]{0,}|\bWEB\b[\s]{0,}[\\bSITE\\b]{0,}[:\s]{0,}){0,}(?:https?://[w? ]{1,}[\.\s]{1,}|[w?]{1,}[\.]{1,})(?:[-A-Z0-9+&@#\/%=~_|$? !:,.]+\bcom\b|[-A-Z0-9+&@#\/%=~_|$?!: ,.]+\.|[-A-Z0-9+&@#\/%=~_|$?!:,.]+[\.]{0,})[^\s,;]{0,})"

regex_pc = r"(?i)([ABCEGHJ-NPRSTVXY][\d][ABCEGHJ-NPRSTV-Z][ -]{0,}[\d][ABCEGHJ-NPRSTV-Z][\d]|[ABCEGHJ-NPRSTVXY?][\d?][ABCEGHJ-NPRSTV-Z?][ -]{1,}[\d?][ABCEGHJ-NPRSTV-Z?][\d?])"

d2p2_table = "caz_amltm_secure.ml_symcor_parsed_d2p2"
d3p2_table = "caz_amltm_secure.ml_symcor_parsed_d3p2"
d4p3_table = "caz_amltm_secure.ml_symcor_parsed_d4p3"



