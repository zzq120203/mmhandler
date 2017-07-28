package iie.mm.dao;

public class WXMessage {
	private long m_chat_room;
	private long m_ch_id;
	private long u_ch_id;
	private String m_mm_audio_txt;
	
	public WXMessage() {
		super();
	}

	public WXMessage(long m_chat_room, long m_ch_id, long u_ch_id, String m_mm_audio_txt) {
		super();
		this.m_chat_room = m_chat_room;
		this.m_ch_id = m_ch_id;
		this.u_ch_id = u_ch_id;
		this.m_mm_audio_txt = m_mm_audio_txt;
	}

	public long getM_chat_room() {
		return m_chat_room;
	}

	public void setM_chat_room(long m_chat_room) {
		this.m_chat_room = m_chat_room;
	}

	public long getM_ch_id() {
		return m_ch_id;
	}

	public void setM_ch_id(long m_ch_id) {
		this.m_ch_id = m_ch_id;
	}

	public long getU_ch_id() {
		return u_ch_id;
	}

	public void setU_ch_id(long u_ch_id) {
		this.u_ch_id = u_ch_id;
	}

	public String getM_mm_audio_txt() {
		return m_mm_audio_txt;
	}

	public void setM_mm_audio_txt(String m_mm_audio_txt) {
		this.m_mm_audio_txt = m_mm_audio_txt;
	}
	
}
