package com.yahoo.ycsb.workloads;

/**
 * Created by frojala on 19/08/16.
 */
import com.gemstone.gemfire.InvalidDeltaException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

//public class UE implements Serializable {
public class UE implements com.gemstone.gemfire.Delta, Serializable {
  /**
   *  Static entity codes, will never change in this benchmark.
   */
  private static final String MNC = "244";
  private static final String MCC = "921";
  private static final String PLMN_ID = MNC + MCC;
  private static final String MMEGI = "M";
  private static final String MMEC = "C";
  private static final String MMEI = MMEGI + MMEC;
  private static final String GUMMEI = PLMN_ID + MMEI;
  public static final String PGW_ID = "ubiquitous.internet";
  private static final int TAI_SIZE = 15;

  /**
   *  Semi-static entity codes. Once generated on insert, will not be changed.
   */
  private String IMEI;
  private String MSIN;
  private String IMSI; // = PLMN_ID + MSIN;
  private String EPS_KEY; // = 128 bit (8 chars)
  private String Cipher_KEY; // = 128 bit
  private String Encryption_KEY; // = 128 bit

  /**
   *  Dynamic entities, will experience change on a regular basis.
   */
  private byte status; // 0 = Detached, 1 = Attached, 2 = active, 3 = idle;
  private int M_TMSI;
  private String GUTI; // = GUMMEI + M_TMSI;
  private String TIN;
  private int IP;
  private short C_RNTI;
  private int eNB_UE_S1AP;
  private int MME_UE_S1AP;
  private int OLD_eNB_UE_X2;
  private int NEW_eNB_UE_X2;
  private int ECI;
  private String ECGI; // = PLMN_ID + ECI;
  private int TAI;
  private List<Integer> TAI_list;
  private String PDN_ID;
  private byte EPS_bearer;
  private byte E_RA_bearer;
  private byte DR_bearer;
  private int S1_TEID_UL;
  private int S1_TEID_DL;
  private int S5_TEID_UL;
  private int S5_TEID_DL;
  private String K_ASME; // = 256 bit (16 chars)
  private String K_ENB; // = 256 bit
  private String K_NASint; // = 256 bit
  private String K_NASenc; // = 256 bit
  private String K_RRCint; // = 256 bit
  private String K_RRCenc; // = 256 bit
  private String K_UPenc; // = 256 bit

  /**
   * The transients to support deltas.
   */
  private transient boolean master_ch;
  private transient boolean status_ch;
  private transient boolean M_TMSI_ch;
  private transient boolean GUTI_ch;
  private transient boolean TIN_ch;
  private transient boolean IP_ch;
  private transient boolean C_RNTI_ch;
  private transient boolean eNB_UE_S1AP_ch;
  private transient boolean MME_YE_S1AP_ch;
  private transient boolean OLD_eNB_UE_X2_ch;
  private transient boolean NEW_eNB_UE_X2_ch;
  private transient boolean ECI_ch;
  private transient boolean ECGI_ch;
  private transient boolean TAI_ch;
  private transient boolean TAI_list_ch;
  private transient boolean PDN_ID_ch;
  private transient boolean EPS_bearer_ch;
  private transient boolean E_RA_bearer_ch;
  private transient boolean DR_bearer_ch;
  private transient boolean S1_TEID_UL_ch;
  private transient boolean S1_TEID_DL_ch;
  private transient boolean S5_TEID_UL_ch;
  private transient boolean S5_TEID_DL_ch;
  private transient boolean K_ASME_ch;
  private transient boolean K_ENB_ch;
  private transient boolean K_NASint_ch;
  private transient boolean K_NASenc_ch;
  private transient boolean K_RRCint_ch;
  private transient boolean K_RRCenc_ch;
  private transient boolean K_UPenc_ch;

  public UE(String IMSI) {
    Random rand = new Random();
    this.IMEI = randomString(15, rand);
    this.IMSI = IMSI;
    this.MSIN = IMSI.substring(6);
    this.EPS_KEY = randomString(8, rand);
    this.Cipher_KEY = randomString(8, rand);
    this.Encryption_KEY = randomString(8, rand);
    this.status = 0; // default detached
  }

  public void initial_attach() {
    Random rand = new Random();
    this.status = 2;  /* Active */                                this.status_ch = true;
    this.M_TMSI = rand.nextInt();                                 this.M_TMSI_ch = true;
    this.GUTI = GUMMEI + M_TMSI;                                  this.GUTI_ch = true;
    this.TIN = "";                                                this.TIN_ch = true;
    this.IP = rand.nextInt();                                     this.IP_ch = true;
    this.C_RNTI = (short) rand.nextInt(Short.MAX_VALUE - 1);      this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = rand.nextInt();                            this.eNB_UE_S1AP_ch = true;
    this.MME_UE_S1AP = rand.nextInt();                            this.MME_YE_S1AP_ch = true;
    this.ECI = rand.nextInt();                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                                    this.ECGI_ch = true;
    this.TAI = rand.nextInt();                                    this.TAI_ch = true;
    this.TAI_list = new ArrayList<>();                            this.TAI_list_ch = true;
    for (int i = 0; i < TAI_SIZE; i++) TAI_list.add(rand.nextInt());
    this.PDN_ID = rand.nextInt() + ".apn.epc.mnc" + MNC + ".mcc" + MCC + rand.nextInt() + "3gppnetwork.org";
    this.PDN_ID_ch = true;
    this.EPS_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);    this.EPS_bearer_ch = true;
    this.E_RA_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);   this.E_RA_bearer_ch = true;
    this.DR_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);     this.DR_bearer_ch = true;
    this.S1_TEID_UL = rand.nextInt();                             this.S1_TEID_UL_ch = true;
    this.S1_TEID_DL = rand.nextInt();                             this.S1_TEID_DL_ch = true;
    this.S5_TEID_UL = rand.nextInt();                             this.S5_TEID_UL_ch = true;
    this.S5_TEID_DL = rand.nextInt();                             this.S5_TEID_DL_ch = true;
    this.K_ASME = randomString(16, rand);                         this.K_ASME_ch = true;
    this.K_ENB = randomString(16, rand);                          this.K_ENB_ch = true;
    this.K_NASint = randomString(16, rand);                       this.K_NASint_ch = true;
    this.K_NASenc = randomString(16, rand);                       this.K_NASenc_ch = true;
    this.K_RRCint = randomString(16, rand);                       this.K_RRCint_ch = true;
    this.K_RRCenc = randomString(16, rand);                       this.K_RRCenc_ch = true;
    this.K_UPenc = randomString(16, rand);                        this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void detach() {
    this.status = 0; /* Detached */                  this.status_ch = true;
    this.IP = 0;                                     this.IP_ch = true;
    this.C_RNTI = 0;                                 this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = 0;                            this.eNB_UE_S1AP_ch = true;
    this.MME_UE_S1AP = 0;                            this.MME_YE_S1AP_ch = true;
    this.ECI = 0;                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                       this.ECGI_ch = true;
    this.PDN_ID = "";                                this.PDN_ID_ch = true;
    this.EPS_bearer = 0;                             this.EPS_bearer_ch = true;
    this.E_RA_bearer = 0;                            this.E_RA_bearer_ch = true;
    this.DR_bearer = 0;                              this.DR_bearer_ch = true;
    this.S1_TEID_UL = 0;                             this.S1_TEID_UL_ch = true;
    this.S1_TEID_DL = 0;                             this.S1_TEID_DL_ch = true;
    this.S5_TEID_UL = 0;                             this.S5_TEID_UL_ch = true;
    this.S5_TEID_DL = 0;                             this.S5_TEID_DL_ch = true;
    this.K_ASME = "";                                this.K_ASME_ch = true;
    this.K_ENB = "";                                 this.K_ENB_ch = true;
    this.K_RRCint = "";                              this.K_RRCint_ch = true;
    this.K_RRCenc = "";                              this.K_RRCenc_ch = true;
    this.K_UPenc = "";                               this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void S1_release() {
    this.status = 3; /* Idle */                      this.status_ch = true;
    this.C_RNTI = 0;                                 this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = 0;                            this.eNB_UE_S1AP_ch = true;
    this.MME_UE_S1AP = 0;                            this.MME_YE_S1AP_ch = true;
    this.ECI = 0;                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                       this.ECGI_ch = true;
    this.DR_bearer = 0;                              this.DR_bearer_ch = true;
    this.S1_TEID_DL = 0;                             this.S1_TEID_DL_ch = true;
    this.K_ENB = "";                                 this.K_ENB_ch = true;
    this.K_RRCint = "";                              this.K_RRCint_ch = true;
    this.K_RRCenc = "";                              this.K_RRCenc_ch = true;
    this.K_UPenc = "";                               this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void service_request() {
    Random rand = new Random();
    this.status = 2; /* Active */                                 this.status_ch = true;
    this.C_RNTI = (short) rand.nextInt(Short.MAX_VALUE - 1);      this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = rand.nextInt();                            this.eNB_UE_S1AP_ch = true;
    this.MME_UE_S1AP = rand.nextInt();                            this.MME_YE_S1AP_ch = true;
    this.ECI = rand.nextInt();                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                                    this.ECGI_ch = true;
    this.DR_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);     this.DR_bearer_ch = true;
    this.S1_TEID_DL = rand.nextInt();                             this.S1_TEID_DL_ch = true;
    this.K_ENB = randomString(16, rand);                          this.K_ENB_ch = true;
    this.K_RRCint = randomString(16, rand);                       this.K_RRCint_ch = true;
    this.K_RRCenc = randomString(16, rand);                       this.K_RRCenc_ch = true;
    this.K_UPenc = randomString(16, rand);                        this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void tracking_area_update() {
    Random rand = new Random();
    this.M_TMSI = rand.nextInt();           this.M_TMSI_ch = true;
    this.GUTI = GUMMEI + M_TMSI;            this.GUTI_ch = true;
    this.TIN = GUTI;                        this.TIN_ch = true;
    this.TAI = rand.nextInt();              this.TAI_ch = true;
    this.TAI_list = new ArrayList<>();      this.TAI_list_ch = true;
    for (int i = 0; i < TAI_SIZE; i++) { TAI_list.add(rand.nextInt()); }
    this.master_ch = true;
  }

  public void handover() {
    // in intra-LTE the information entites are the same as in X2 handover. T
    // This is a mockup, as in the deployment scenario we envision there to be no need for this.
    Random rand = new Random();
    this.S1_TEID_DL = rand.nextInt();                             this.S1_TEID_DL_ch = true;
    this.DR_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);     this.DR_bearer_ch = true;
    this.ECI = rand.nextInt();                                    this.ECI_ch = true;
    this.ECGI = PLMN_ID + ECI;                                    this.ECGI_ch = true;
    this.C_RNTI = (short) rand.nextInt(Short.MAX_VALUE - 1);      this.C_RNTI_ch = true;
    this.eNB_UE_S1AP = rand.nextInt();                            this.eNB_UE_S1AP_ch = true;
    this.K_ENB = randomString(16, rand);                          this.K_ENB_ch = true;
    this.K_RRCint = randomString(16, rand);                       this.K_RRCint_ch = true;
    this.K_RRCenc = randomString(16, rand);                       this.K_RRCenc_ch = true;
    this.K_UPenc = randomString(16, rand);                        this.K_UPenc_ch = true;
    this.master_ch = true;
  }

  public void session_management() {
    // we mock a session management act by changing the EPS, S1 and S5 TEID for creation of new bearers.
    Random rand = new Random();
    this.EPS_bearer = (byte) rand.nextInt(Byte.MAX_VALUE - 1);    this.EPS_bearer_ch = true;
    this.S1_TEID_UL = rand.nextInt();                             this.S1_TEID_UL_ch = true;
    this.S1_TEID_DL = rand.nextInt();                             this.S1_TEID_DL_ch = true;
    this.S5_TEID_UL = rand.nextInt();                             this.S5_TEID_UL_ch = true;
    this.S5_TEID_DL = rand.nextInt();                             this.S5_TEID_DL_ch = true;
    this.master_ch = true;
  }

  public void cell_reselect() {
    // nothing happens.
  }

  public int getStatus() {
    return this.status;
  }

  public String getIMSI() { return this.IMSI; }

  private String randomString(int chars, Random rand) {
    String s = "";
    String hexa = "0123456789ABCDEF";
    for (int i = 0; i < chars; i++) {
      s += hexa.charAt(rand.nextInt(16));
    }
    return s;
  }

//  @Override
  public boolean hasDelta() {
    return master_ch;
  }

//  @Override
  public void toDelta(DataOutput out) throws IOException {
    out.writeBoolean(status_ch);                if (status_ch){ out.writeByte(status); this.status_ch = false; }
    out.writeBoolean(M_TMSI_ch);                if (M_TMSI_ch) { out.writeInt(M_TMSI); this.M_TMSI_ch = false; }
    out.writeBoolean(GUTI_ch);                  if (GUTI_ch) { out.writeUTF(GUTI); this.GUTI_ch = false; }
    out.writeBoolean(TIN_ch);                   if (TIN_ch) { out.writeUTF(TIN); this.TIN_ch = false; }
    out.writeBoolean(IP_ch);                    if (IP_ch) { out.writeInt(IP); this.IP_ch = false; }
    out.writeBoolean(C_RNTI_ch);                if (C_RNTI_ch) { out.writeShort(C_RNTI); this.C_RNTI_ch = false; }
    out.writeBoolean(eNB_UE_S1AP_ch);           if (eNB_UE_S1AP_ch) { out.writeInt(eNB_UE_S1AP); this.eNB_UE_S1AP_ch = false; }
    out.writeBoolean(MME_YE_S1AP_ch);           if (MME_YE_S1AP_ch) { out.writeInt(MME_UE_S1AP); this.MME_YE_S1AP_ch = false; }
    out.writeBoolean(OLD_eNB_UE_X2_ch);         if (OLD_eNB_UE_X2_ch) { out.writeInt(OLD_eNB_UE_X2); this.OLD_eNB_UE_X2_ch = false; }
    out.writeBoolean(NEW_eNB_UE_X2_ch);         if (NEW_eNB_UE_X2_ch) { out.writeInt(NEW_eNB_UE_X2); this.NEW_eNB_UE_X2_ch = false; }
    out.writeBoolean(ECI_ch);                   if (ECI_ch) { out.writeInt(ECI); this.ECI_ch = false; }
    out.writeBoolean(ECGI_ch);                  if (ECGI_ch) { out.writeUTF(ECGI); this.ECGI_ch = false; }
    out.writeBoolean(TAI_ch);                   if (TAI_ch) { out.writeInt(TAI); this.TAI_ch = false; }
    out.writeBoolean(TAI_list_ch);              if (TAI_list_ch) { out.writeInt(TAI_list.size()); for (int TAI:TAI_list) out.writeInt(TAI); this.TAI_list_ch = false; }
    out.writeBoolean(PDN_ID_ch);                if (PDN_ID_ch) { out.writeUTF(PDN_ID); this.PDN_ID_ch = false; }
    out.writeBoolean(EPS_bearer_ch);            if (EPS_bearer_ch) { out.writeByte(EPS_bearer); this.EPS_bearer_ch = false; }
    out.writeBoolean(E_RA_bearer_ch);           if (E_RA_bearer_ch) { out.writeByte(E_RA_bearer); this.E_RA_bearer_ch = false; }
    out.writeBoolean(DR_bearer_ch);             if (DR_bearer_ch) { out.writeByte(DR_bearer); this.DR_bearer_ch = false; }
    out.writeBoolean(S1_TEID_DL_ch);            if (S1_TEID_DL_ch) { out.writeInt(S1_TEID_DL); this.S1_TEID_DL_ch = false; }
    out.writeBoolean(S1_TEID_UL_ch);            if (S1_TEID_UL_ch) { out.writeInt(S1_TEID_UL); this.S1_TEID_UL_ch = false; }
    out.writeBoolean(S5_TEID_DL_ch);            if (S5_TEID_DL_ch) { out.writeInt(S5_TEID_DL); this.S5_TEID_DL_ch = false; }
    out.writeBoolean(S5_TEID_UL_ch);            if (S5_TEID_UL_ch) { out.writeInt(S5_TEID_UL); this.S5_TEID_UL_ch = false; }
    out.writeBoolean(K_ASME_ch);                if (K_ASME_ch) { out.writeUTF(K_ASME); this.K_ASME_ch = false; }
    out.writeBoolean(K_ENB_ch);                 if (K_ENB_ch) { out.writeUTF(K_ENB); this.K_ENB_ch = false; }
    out.writeBoolean(K_NASint_ch);              if (K_NASint_ch) { out.writeUTF(K_NASint); this.K_NASint_ch = false; }
    out.writeBoolean(K_NASenc_ch);              if (K_NASenc_ch) { out.writeUTF(K_NASenc); this.K_NASenc_ch = false; }
    out.writeBoolean(K_RRCint_ch);              if (K_RRCint_ch) { out.writeUTF(K_RRCint); this.K_RRCint_ch = false; }
    out.writeBoolean(K_RRCenc_ch);              if (K_RRCenc_ch) { out.writeUTF(K_RRCenc); this.K_RRCenc_ch = false; }
    out.writeBoolean(K_UPenc_ch);               if (K_UPenc_ch) { out.writeUTF(K_UPenc); this.K_UPenc_ch = false; }
    this.master_ch = false;
  }

//  @Override
  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    if (in.readBoolean()) this.status = in.readByte();
    if (in.readBoolean()) this.M_TMSI = in.readInt();
    if (in.readBoolean()) this.GUTI = in.readUTF();
    if (in.readBoolean()) this.TIN = in.readUTF();
    if (in.readBoolean()) this.IP = in.readInt();
    if (in.readBoolean()) this.C_RNTI = in.readShort();
    if (in.readBoolean()) this.eNB_UE_S1AP = in.readInt();
    if (in.readBoolean()) this.MME_UE_S1AP = in.readInt();
    if (in.readBoolean()) this.OLD_eNB_UE_X2 = in.readInt();
    if (in.readBoolean()) this.NEW_eNB_UE_X2 = in.readInt();
    if (in.readBoolean()) this.ECI = in.readInt();
    if (in.readBoolean()) this.ECGI = in.readUTF();
    if (in.readBoolean()) this.TAI = in.readInt();
    if (in.readBoolean()) {
      int size = in.readInt();
      this.TAI_list = new ArrayList<>();
      for (int i = 0; i < size; i++) this.TAI_list.add(in.readInt());
    }
    if (in.readBoolean()) this.PDN_ID = in.readUTF();
    if (in.readBoolean()) this.EPS_bearer = in.readByte();
    if (in.readBoolean()) this.E_RA_bearer = in.readByte();
    if (in.readBoolean()) this.DR_bearer = in.readByte();
    if (in.readBoolean()) this.S1_TEID_DL = in.readInt();
    if (in.readBoolean()) this.S1_TEID_UL = in.readInt();
    if (in.readBoolean()) this.S5_TEID_DL = in.readInt();
    if (in.readBoolean()) this.S5_TEID_UL = in.readInt();
    if (in.readBoolean()) this.K_ASME = in.readUTF();
    if (in.readBoolean()) this.K_ENB = in.readUTF();
    if (in.readBoolean()) this.K_NASint = in.readUTF();
    if (in.readBoolean()) this.K_NASenc = in.readUTF();
    if (in.readBoolean()) this.K_RRCint = in.readUTF();
    if (in.readBoolean()) this.K_RRCenc = in.readUTF();
    if (in.readBoolean()) this.K_UPenc = in.readUTF();
  }

}

