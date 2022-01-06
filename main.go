package main

import (
	sap_api_caller "sap-api-integrations-business-partner-reads-customer-rmq-kube/SAP_API_Caller"
	sap_api_input_reader "sap-api-integrations-business-partner-reads-customer-rmq-kube/SAP_API_Input_Reader"
	"sap-api-integrations-business-partner-reads-customer-rmq-kube/config"

	"github.com/latonaio/golang-logging-library/logger"
	rabbitmq "github.com/latonaio/rabbitmq-golang-client"
	"golang.org/x/xerrors"
)

func main() {
	l := logger.NewLogger()
	conf := config.NewConf()
	rmq, err := rabbitmq.NewRabbitmqClient(conf.RMQ.URL(), conf.RMQ.QueueFrom(), conf.RMQ.QueueTo())
	if err != nil {
		l.Fatal(err.Error())
	}
	defer rmq.Close()

	caller := sap_api_caller.NewSAPAPICaller(
		conf.SAP.BaseURL(),
		conf.RMQ.QueueTo(),
		rmq,
		l,
	)

	iter, err := rmq.Iterator()
	if err != nil {
		l.Fatal(err.Error())
	}
	defer rmq.Stop()

	for msg := range iter {
		err = callProcess(caller, msg)
		if err != nil {
			msg.Fail()
			l.Error(err)
			continue
		}
		msg.Success()
	}
}

func callProcess(caller *sap_api_caller.SAPAPICaller, msg rabbitmq.RabbitmqMessage) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = xerrors.Errorf("error occurred: %w", e)
			return
		}
	}()
	businessPartner, businessPartnerRole, addressID, bankCountryKey, bankNumber, bPName, customer, salesOrganization, distributionChannel, division, companyCode := extractData(msg.Data())
	accepter := getAccepter(msg.Data())
	caller.AsyncGetBPCustomer(businessPartner, businessPartnerRole, addressID, bankCountryKey, bankNumber, bPName, customer, salesOrganization, distributionChannel, division, companyCode, accepter)
	return nil
}

func extractData(data map[string]interface{}) (businessPartner, businessPartnerRole, addressID, bankCountryKey, bankNumber, bPName, customer, salesOrganization, distributionChannel, division, companyCode string) {
	sdc := sap_api_input_reader.ConvertToSDC(data)
	businessPartner = sdc.BusinessPartner.BusinessPartner
	businessPartnerRole = sdc.BusinessPartner.Role.BusinessPartnerRole
	addressID = sdc.BusinessPartner.Address.AddressID
	bankCountryKey = sdc.BusinessPartner.Bank.BankCountryKey
	bankNumber = sdc.BusinessPartner.Bank.BankNumber
	bPName = sdc.BusinessPartner.BusinessPartnerName
	customer = sdc.BusinessPartner.CustomerData.Customer
	salesOrganization = sdc.BusinessPartner.CustomerData.SalesArea.SalesOrganization
	distributionChannel = sdc.BusinessPartner.CustomerData.SalesArea.DistributionChannel
	division = sdc.BusinessPartner.CustomerData.SalesArea.Division
	companyCode = sdc.BusinessPartner.Company.CompanyCode
	return
}

func getAccepter(data map[string]interface{}) []string {
	sdc := sap_api_input_reader.ConvertToSDC(data)
	accepter := sdc.Accepter
	if len(sdc.Accepter) == 0 {
		accepter = []string{"All"}
	}

	if accepter[0] == "All" {
		accepter = []string{
			"General", "Role", "Address", "Bank", "BPName", "Customer", "SalesArea", "Company",
		}
	}
	return accepter
}
