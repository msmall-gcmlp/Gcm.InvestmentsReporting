import azure.durable_functions as df
import json


def orchestrator_function(context: df.DurableOrchestrationContext):
    requestBody: str = context.get_input()
    inputs_request = requestBody.copy()
    inputs_request = json.dumps(inputs_request)
    yield context.call_activity("PerformanceQualityInputsActivity", inputs_request)

    fund_names = ['AKO Global', 'ARCM II', 'Advanced Life Science', 'Advent Glbl Ptnrs', 'Agilon', 'Algebris NPL Fund II', 'Alphadyne Global Rates II', 
    'Alphadyne Intl Master', 'Altimeter', 'Anatole', 'Anchorage Cap', 'Antara Capital', 'Apollo Accord IV', 'Apollo Credit Strategies Fund', 'Aspect', 
    'Aspex', 'Atlas Enhanced Fund', 'Atlas Global', 'Avidity', 'BCP Special Opp II', 'BVF', 'BVF Invest 10', 'Bayview MSR', 'Bayview Opp Fund VI', 
    'BlackRock EC Fund', 'BlackRock Strategic', 'Braidwell', 'Brevan Howard FG Macro', 'CRF III', 'Candlestick', 'Canyon Balanced', 'Canyon Opp Cred', 
    'Canyon VRF', 'CapitalSpring V II', 'Capula Tactical Macro', 'CarVal Global Credit Fund', 'CarVal Intl Credit', 'CarVal Intl Credit II', 
    'Cerberus Global NPL Fund', 'Charlesbank Credit Opp II', 'Chenavari EOCF II', 'Chenavari Opp Credit', 'Chenavari Struct Credit', 'Cheyne RE Credit', 
    'Cheyne RE Credit III', 'Cheyne RE Credit V', 'Citadel', 'Citadel Global Equities', 'Coatue', 'Concordia G-10 FIRV', 'CoreView', 'Corre Opportunities', 
    'D1 Capital', 'DE Shaw', 'DE Shaw Alkali Fund IV', 'DE Shaw Alkali Fund V', 'Davidson Kempner', 'Davidson Kempner LT Distressed Opp V', 'Deep Track', 
    'Diameter Dislocation', 'Diameter Main Fund', 'Dragon Billion Select', 'Dragoneer Global Fund', 'Egerton', 'Element', 'Elizabeth Park', 'Elliott', 
    'Envoy', 'Eversept ELS', 'Exodus Point', 'FCOI II', 'Fairway', 'Farallon', 'Fidera', 'GCM Asia SubFund', 'GCM Macro SubFund', 'GCM OCFIV', 
    'GCM Special Opps Fund', 'GCM Spectrum Fund', 'GDOF III', 'GSO Energy Opp', 'H.I.G. Bayside Loan VI', 'HBK', 'Hawk Ridge', 'Hawksbridge', 
    'Heard Opportunity LS', 'Hel Ved', 'Hollis Park', 'Holocene', 'Impactive Capital', 'InSolve Global Cred IV', 'Insight Partners Public Equities (IPPE)', 
    'Ishana', 'J-Wellness', 'Japan-Up', 'Jin Japan', 'Kadensa', 'Kennedy Lewis Cap II', 'Kennedy Lewis Fund I', 'Kinetic Partners', 'King Street', 
    'LMR Master', 'Lake Bleu', 'Laurion', 'Linden', 'Lynrock Lake', 'MAC II', 'MY Asian Opps', 'Magnetar Aviation', 'Magnetar Constellation', 
    'Magnetar Constellation V', 'Magnetar Energy', 'Magnetar Energy Opp', 'Magnetar PRA', 'Maplelane', 'Marshall Wace Eureka Fund', 'Melvin Capital', 
    'Napier Park ABS Income Fund', 'Northwest', 'OHA Partners', 'Oceanic Opportunity', 'Owl Rock Tech Finance', 'PAG', 'PIMCO Global IG', 'PIMCO Tactical', 
    'PIMCO US IG Credit Bond', 'Pantheum', 'Parsifal', 'Pathfinder Strategic Credit', 'Pathfinder Strategic Credit II', 'Pentwater Credit', 'Pentwater Equity Opp', 
    'Pentwater Event', 'Pentwater Merger Arb', 'Pharo Gaia', 'Pharo Macro Fund', 'Pharo Trading', 'PineBridge India', 'Point72', 'PointState SteelMill', 
    'Praetor Fund I', 'RIDA', 'RIDGE', 'RIEF', 'ReadyState', 'Recompense', 'Recompense II', 'RedCo', 'RedCo II', 'Redmile', 'Redwood', 'Rokos', 'SEG', 
    'SILQ', 'SRS Partners', 'Schonfeld Strategic Partners', 'Sculptor Credit Opps', 'Sculptor II', 'Segantii Asia Pac', 'Seiga Japan', 'Select Equity ELS', 
    'Shaolin Capital Partners', 'Shelter Growth Int Trm', 'Shelter Growth Int Trm II', 'Shelter Growth IntTrm III', 'Shelter Growth Opp', 'Silver Point', 
    'Simplex Oyako', 'Skye', 'Snowcat', 'Snowhook Capital', 'Southpoint', 'Spectrum', 'Standard General II', 'Steadfast', 'Suvretta', 'TCW MetWest Unconstrained', 
    'TPG Public Eq Long Opps', 'TPGEquity', 'Tairen Alpha', 'Tamarack Gbl Healthcare', 'Tensile', 'Tewksbury', 'Tiger Global', 'Tor Asia Credit', 'Trivest', 
    'VR Global', 'Valiant Peregrine Fund 2', 'Varadero', 'Vista Credit Fund 3', 'Voleon Inst Strat Intl', 'Voleon Intl Inv', 'Voloridge', 'Voyager', 'WT Capital', 
    'Waterfall Victoria', 'Wexford Catalyst', 'Whale Rock', 'Whale Rock Hybrid Long/Short', 'Whitebox Relative Value', 'Whitehaven Credit Opp', 'Woodline Partners', 
    'York Asia']

    parallel_tasks = []
    for fund in fund_names:
        params = requestBody.copy()
        params['params']['fund_name'] = fund
        params = json.dumps(params)
        parallel_tasks.append(context.call_activity(
            "PerformanceQualityReportActivity", params
        ))
    yield context.task_all(parallel_tasks)


main = df.Orchestrator.create(orchestrator_function)
