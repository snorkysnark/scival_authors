from ralsei import ValueColumn

authors_fields = {
    "Name": ValueColumn("name", "TEXT"),
    "Scholarly Output": ValueColumn("scholarly_output", "INT"),
    "Most recent publication": ValueColumn("most_recent_publication", "INT"),
    "Citations": ValueColumn("citations", "INT"),
    "Citations per Publication": ValueColumn("citations_per_publication", "INT"),
    "Field-Weighted Citation Impact": ValueColumn(
        "field_weighted_citation_impact", "FLOAT"
    ),
    "h-index": ValueColumn("h_index", "INT"),
    "Output in Top 10% Citation Percentiles (field-weighted)": ValueColumn(
        "output_in_top_10_citation_percentiles", "INT"
    ),
    "Oldest publication (since 1996)": ValueColumn("oldest_publication", "INT"),
    "Scopus author ID": ValueColumn("scopus_author_id", "INT"),
    "Scopus author profile": ValueColumn("scopus_author_profile", "TEXT"),
    "Primary author affiliation": ValueColumn("primary_author_affiliation", "TEXT"),
}
publication_fields = {
    "Title": ValueColumn("title", "TEXT"),
    "Authors": ValueColumn("authors", "TEXT"),
    "Number of Authors": ValueColumn("number_of_authors", "INT"),
    "Scopus Author Ids": ValueColumn("scopus_author_ids", "TEXT"),
    "Year": ValueColumn("year", "INT"),
    "Full date": ValueColumn("full_date", "TEXT"),
    "Scopus Source title": ValueColumn("scopus_source_title", "TEXT"),
    "Volume": ValueColumn("volume", "TEXT"),
    "Issue": ValueColumn("issue", "TEXT"),
    "Pages": ValueColumn("pages", "TEXT"),
    "Article number": ValueColumn("article_number", "TEXT"),
    "ISSN": ValueColumn("issn", "TEXT"),
    "Source ID": ValueColumn("source_id", "INT"),
    "Source type": ValueColumn("source_type", "TEXT"),
    "Language": ValueColumn("language", "TEXT"),
    "SNIP (publication year)": ValueColumn("snip", "TEXT"),
    "SNIP percentile (publication year) *": ValueColumn("snip_percentile", "TEXT"),
    "CiteScore (publication year)": ValueColumn("citescore", "TEXT"),
    "CiteScore percentile (publication year) *": ValueColumn(
        "citescore_percentile", "TEXT"
    ),
    "SJR (publication year)": ValueColumn("sjr", "TEXT"),
    "SJR percentile (publication year) *": ValueColumn("sjr_percentile", "TEXT"),
    "Field-Weighted View Impact": ValueColumn("field_weighted_view_impact", "FLOAT"),
    "Views": ValueColumn("views", "INT"),
    "Citations": ValueColumn("citations", "INT"),
    "Field-Weighted Citation Impact": ValueColumn(
        "field_weighted_citation_impact", "FLOAT"
    ),
    "Field-Citation Average": ValueColumn("field_citation_average", "FLOAT"),
    "Outputs in Top Citation Percentiles, per percentile": ValueColumn(
        "outputs_in_top_citation_percentiles_per_percintile", "INT"
    ),
    "Field-Weighted Outputs in Top Citation Percentiles, per percentile": ValueColumn(
        "field_weighted_outputs_in_top_citation_percentiles_per_percentile", "INT"
    ),
    "Patent citations": ValueColumn("patent_citations", "INT"),
    "Media citations": ValueColumn("media_citations", "INT"),
    "Reference": ValueColumn("reference", "TEXT"),
    "Abstract": ValueColumn("abstract", "TEXT"),
    "DOI": ValueColumn("doi", "TEXT"),
    "Publication type": ValueColumn("publication_type", "TEXT"),
    "Open Access": ValueColumn("open_access", "TEXT"),
    "EID": ValueColumn("eid", "TEXT UNIQUE"),
    "PubMed ID": ValueColumn("pubmed_id", "TEXT"),
    "Institutions": ValueColumn("institutions", "TEXT"),
    "Number of Institutions": ValueColumn("number_of_institutions", "INT"),
    "Scopus Affiliation IDs": ValueColumn("scopus_affiliation_ids", "TEXT"),
    "Scopus Affiliation names": ValueColumn("scopus_affiliation_names", "TEXT"),
    "Scopus Author ID First Author": ValueColumn(
        "scopus_author_id_first_author", "TEXT"
    ),
    "Scopus Author ID Last Author": ValueColumn("scopus_author_id_last_author", "TEXT"),
    "Scopus Author ID Corresponding Author": ValueColumn(
        "scopus_author_id_corresponding_author", "TEXT"
    ),
    "Scopus Author ID Single Author": ValueColumn(
        "scopus_author_id_single_author", "TEXT"
    ),
    "Country/Region": ValueColumn("country_region", "TEXT"),
    "Number of Countries/Regions": ValueColumn("number_of_countries_regions", "INT"),
    "All Science Journal Classification (ASJC) code": ValueColumn("asjc_code", "TEXT"),
    "All Science Journal Classification (ASJC) field name": ValueColumn(
        "asjc_field_name", "TEXT"
    ),
    "Quacquarelli Symonds (QS) Subject area code": ValueColumn(
        "qs_subject_area_code", "TEXT"
    ),
    "Quacquarelli Symonds (QS) Subject area field name": ValueColumn(
        "qs_subject_area_field_name", "TEXT"
    ),
    "Quacquarelli Symonds (QS) Subject code": ValueColumn("qs_subject_code", "TEXT"),
    "Quacquarelli Symonds (QS) Subject field name": ValueColumn(
        "qs_subject_field_name", "TEXT"
    ),
    "Times Higher Education (THE) code": ValueColumn("the_code", "TEXT"),
    "Times Higher Education (THE) field name": ValueColumn("the_field_name", "TEXT"),
    "ANZSRC FoR (2020) parent code": ValueColumn("anzsrc_for_parent_code", "TEXT"),
    "ANZSRC FoR (2020) parent name": ValueColumn("anzsrc_for_parent_name", "TEXT"),
    "ANZSRC FoR (2020) code": ValueColumn("anzsrc_for_code", "TEXT"),
    "ANZSRC FoR (2020) name": ValueColumn("anzsrc_for_name", "TEXT"),
    "Sustainable Development Goals (2023)": ValueColumn(
        "sustainable_development_goals", "TEXT"
    ),
    "Topic Cluster name": ValueColumn("topic_cluster_name", "TEXT"),
    "Topic Cluster number": ValueColumn("topic_cluster_number", "TEXT"),
    "Topic name": ValueColumn("topic_name", "TEXT"),
    "Topic number": ValueColumn("topic_number", "TEXT"),
    "Topic Cluster Prominence Percentile": ValueColumn(
        "topic_cluster_prominence_percentile", "TEXT"
    ),
    "Topic Prominence Percentile": ValueColumn("topic_prominence_percentile", "TEXT"),
}
