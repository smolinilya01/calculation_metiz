SELECT (SELECT TOP (1) ListOfSectorsOfProdOnOrdersForPlaning.DATE_BEGIN 
  FROM ListOfSectorsOfProdOnOrdersForPlaning, HistoryObjectsOfDogSpec
  WHERE ListOfSectorsOfProdOnOrdersForPlaning.ID_ORDER = HistoryObjectsOfDogSpec.ID_HISTORY_OBJECTS
  AND HistoryObjectsOfDogSpec.ID_ORDER = HistoryOfOrdersParams.IDZAKAZA 
  AND ListOfSectorsOfProdOnOrdersForPlaning.TYPE_ORDER = 3
  AND ListOfSectorsOfProdOnOrdersForPlaning.ID_SECTOR = 23
  AND ListOfSectorsOfProdOnOrdersForPlaning.DATE_BEGIN IS NOT NULL
  ORDER BY ListOfSectorsOfProdOnOrdersForPlaning.DATE_BEGIN) AS DATE_START_PROD,
  
CASE WHEN sprGroupKontrAgentov.NUMBERGROUPKONTRAGENTOV != 10 THEN 
  RIGHT(YEAR(HistoryZakazov.CURDATE), 1) + 
  CAST (sprGroupKontrAgentov.NUMBERGROUPKONTRAGENTOV AS VARCHAR) + 
  CASE WHEN LEN(HistoryZakazov.NUMZAKAZA) < 2 THEN '0' + 
  CAST (HistoryZakazov.NUMZAKAZA AS VARCHAR) ELSE 
  CAST (HistoryZakazov.NUMZAKAZA AS VARCHAR) END ELSE 
  HistoryZakazov.NUMZAKAZA END AS NUMBER_ORDER,

ISNULL(ListOfProductionsByRequest.LEVEL_OF_ALLOWING, 0) AS LEVEL_OF_ALLOWING

FROM ListOfProductionsByRequest, PreRegistryZakazov, HistoryZakazov, sprGroupKontrAgentov, HistoryOfOrdersParams 

WHERE ListOfProductionsByRequest.IDREGISTRY = PreRegistryZakazov.IDREGISTRY

/*Отсеиваем спецификации на оцинковку давальческих м/к*/
/*AND ISNULL(PreRegistryZakazov.TYPE_OF_RECORD, 1) = 1*/

AND sprGroupKontrAgentov.IDGROUPKONTRAGENTOV = HistoryZakazov.IDGROUPKONTRAGENTOV 
AND HistoryOfOrdersParams.IDZAKAZA = HistoryZakazov.IDZAKAZA 
AND HistoryOfOrdersParams.ID_NAME_PROD_ON_REQUEST = ListOfProductionsByRequest.ID_NAME_PRODUCTION

AND (CASE WHEN sprGroupKontrAgentov.NUMBERGROUPKONTRAGENTOV != 10 THEN 
  RIGHT(YEAR(HistoryZakazov.CURDATE), 1) + 
  CAST (sprGroupKontrAgentov.NUMBERGROUPKONTRAGENTOV AS VARCHAR) + 
  CASE WHEN LEN(HistoryZakazov.NUMZAKAZA) < 2 THEN '0' + 
  CAST (HistoryZakazov.NUMZAKAZA AS VARCHAR) ELSE 
  CAST (HistoryZakazov.NUMZAKAZA AS VARCHAR) END ELSE 
  HistoryZakazov.NUMZAKAZA END) IN {0}

ORDER BY 2