
namespace LostDealReasonCheck {
    class ResultLine {
        public long DealID { set; get; }
        public long OwnerID { set; get; }
        public string LossName { set; get; }

        public ResultLine(long DealID, long OwnerID, string LossName) {
            this.DealID = DealID;
            this.OwnerID = OwnerID;
            this.LossName = LossName;
        }

        public override string ToString() {
            return "deal_id: " + DealID + "\towner_id: " + OwnerID + "\tloss_name: " + LossName;
        }


    }
}
