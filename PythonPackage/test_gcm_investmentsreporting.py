from src.gcm.InvestmentsReporting.Runners.investmentsreporting import (
    InvestemtnsReportingRunner,
)


def test_helloworld_no_params():
    v = InvestemtnsReportingRunner().execute()
    assert v is not None
