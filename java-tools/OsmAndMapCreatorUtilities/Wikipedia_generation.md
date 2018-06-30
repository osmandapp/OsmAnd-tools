Generation steps:

1. ./utilities.sh (WikiDatabasePreparation.main()) generate-wiki-world-sqlite --dir=<path to wikidata dump> --mode=process-wikidata  
    * Generates a wikidata.sqlite database thith the following tables: ```wiki_coords(id text, lat double, lon double)``` and    ```wiki_mapping(id text, lang text, title text)``` from the dump. Estimate generation time 7 hours (100 mb/min)   
2. ./utilities.sh (WikiDatabasePreparation.main()) generate-wiki-world-sqlite --lang=<wiki language> --dir=<path to wikidata dump> --mode=process-wikipedia     
    * Generates wiki.sqlite (the world wiki file), specifically the following table: ```wiki_content(id long, lat double, lon    double, title text, lang text, zipContent blob)``` Uses the coordinates from the first dabase and relies on the wikidata      ids to group translations. Estimate time: 30 hours.
3. ./utilities.sh (WikipediaByCountryDivider.main()) generate-wikipedia-by-country update_countries <path to wiki.sqlite>  
    * Creates table ```wiki_region(id long, regionName text)``` and populates it with mappings id->regionName
4. ./utilities.sh (WikipediaByCountryDivider.main()) generate-wikipedia-by-country generate_country_sqlite <path to wiki.sqlite> --skip-existing (optional)  
    * Splits the world.sqlite into regions and generates the .obf files for each region. Estimate time: 30 hours.
