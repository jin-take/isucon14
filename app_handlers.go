package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQLドライバのインポート
	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
)

const (
	initialFare     = 500 // 初期料金の例
	farePerDistance = 100 // 距離単位あたりの料金の例
)

var db *sqlx.DB // グローバルなデータベース接続（初期化はmain関数で行う）

// Helper Functions

func bindJSON(r *http.Request, dst interface{}) error {
	decoder := json.NewDecoder(r.Body)
	return decoder.Decode(dst)
}

func writeError(w http.ResponseWriter, status int, err error) {
	http.Error(w, err.Error(), status)
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func secureRandomStr(length int) string {
	// セキュアなランダム文字列生成ロジック（実装は省略）
	return "secure_random_string"
}

// Data Structures

type appPostUsersRequest struct {
	Username       string  `json:"username"`
	FirstName      string  `json:"firstname"`
	LastName       string  `json:"lastname"`
	DateOfBirth    string  `json:"date_of_birth"`
	InvitationCode *string `json:"invitation_code"`
}

type appPostUsersResponse struct {
	ID             string `json:"id"`
	InvitationCode string `json:"invitation_code"`
}

type appPostPaymentMethodsRequest struct {
	Token string `json:"token"`
}

type getAppRidesResponse struct {
	Rides []getAppRidesResponseItem `json:"rides"`
}

type getAppRidesResponseItem struct {
	ID                    string                       `json:"id"`
	PickupCoordinate      Coordinate                   `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                   `json:"destination_coordinate"`
	Chair                 getAppRidesResponseItemChair `json:"chair"`
	Fare                  int                          `json:"fare"`
	Evaluation            int                          `json:"evaluation"`
	RequestedAt           int64                        `json:"requested_at"`
	CompletedAt           int64                        `json:"completed_at"`
}

type getAppRidesResponseItemChair struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Model string `json:"model"`
}

type Coordinate struct {
	Latitude  int `json:"latitude"`
	Longitude int `json:"longitude"`
}

type User struct {
	ID             string `db:"id"`
	Username       string `db:"username"`
	FirstName      string `db:"firstname"`
	LastName       string `db:"lastname"`
	DateOfBirth    string `db:"date_of_birth"`
	AccessToken    string `db:"access_token"`
	InvitationCode string `db:"invitation_code"`
}

type Coupon struct {
	ID        string    `db:"id"`
	UserID    string    `db:"user_id"`
	Code      string    `db:"code"`
	Discount  int       `db:"discount"`
	UsedBy    *string   `db:"used_by"`
	CreatedAt time.Time `db:"created_at"`
}

type Ride struct {
	ID                   string         `db:"id"`
	UserID               string         `db:"user_id"`
	PickupLatitude       int            `db:"pickup_latitude"`
	PickupLongitude      int            `db:"pickup_longitude"`
	DestinationLatitude  int            `db:"destination_latitude"`
	DestinationLongitude int            `db:"destination_longitude"`
	ChairID              sql.NullString `db:"chair_id"`
	Evaluation           *int           `db:"evaluation"`
	CreatedAt            time.Time      `db:"created_at"`
	UpdatedAt            time.Time      `db:"updated_at"`
}

type RideStatus struct {
	ID        string       `db:"id"`
	RideID    string       `db:"ride_id"`
	Status    string       `db:"status"`
	CreatedAt time.Time    `db:"created_at"`
	AppSentAt sql.NullTime `db:"app_sent_at"`
}

type Chair struct {
	ID       string `db:"id"`
	Name     string `db:"name"`
	Model    string `db:"model"`
	OwnerID  string `db:"owner_id"`
	IsActive bool   `db:"is_active"`
}

type Owner struct {
	ID   string `db:"id"`
	Name string `db:"name"`
}

type PaymentToken struct {
	UserID string `db:"user_id"`
	Token  string `db:"token"`
}

type appPostRidesRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesResponse struct {
	RideID string `json:"ride_id"`
	Fare   int    `json:"fare"`
}

type appPostRideEvaluationRequest struct {
	Evaluation int `json:"evaluation"`
}

type appPostRideEvaluationResponse struct {
	CompletedAt int64 `json:"completed_at"`
}

type appGetNotificationResponse struct {
	Data         *appGetNotificationResponseData `json:"data"`
	RetryAfterMs int                             `json:"retry_after_ms"`
}

type appGetNotificationResponseData struct {
	RideID                string                           `json:"ride_id"`
	PickupCoordinate      Coordinate                       `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                       `json:"destination_coordinate"`
	Fare                  int                              `json:"fare"`
	Status                string                           `json:"status"`
	Chair                 *appGetNotificationResponseChair `json:"chair,omitempty"`
	CreatedAt             int64                            `json:"created_at"`
	UpdateAt              int64                            `json:"updated_at"`
}

type appGetNotificationResponseChair struct {
	ID    string                               `json:"id"`
	Name  string                               `json:"name"`
	Model string                               `json:"model"`
	Stats appGetNotificationResponseChairStats `json:"stats"`
}

type appGetNotificationResponseChairStats struct {
	TotalRidesCount    int     `json:"total_rides_count"`
	TotalEvaluationAvg float64 `json:"total_evaluation_avg"`
}

type appGetNearbyChairsResponse struct {
	Chairs      []appGetNearbyChairsResponseChair `json:"chairs"`
	RetrievedAt int64                             `json:"retrieved_at"`
}

type appGetNearbyChairsResponseChair struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Model             string     `json:"model"`
	CurrentCoordinate Coordinate `json:"current_coordinate"`
}

// Handler Functions

func appPostUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostUsersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Username == "" || req.FirstName == "" || req.LastName == "" || req.DateOfBirth == "" {
		writeError(w, http.StatusBadRequest, errors.New("required fields(username, firstname, lastname, date_of_birth) are empty"))
		return
	}

	userID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	invitationCode := secureRandomStr(15)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// 一度のクエリでユーザーとクーポンを挿入
	query := `
		INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code) 
		VALUES (?, ?, ?, ?, ?, ?, ?);
		INSERT INTO coupons (user_id, code, discount) VALUES (?, 'CP_NEW2024', 3000);
	`
	_, err = tx.ExecContext(ctx, query, userID, req.Username, req.FirstName, req.LastName, req.DateOfBirth, accessToken, invitationCode, userID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 招待コードが提供されている場合の処理
	if req.InvitationCode != nil && *req.InvitationCode != "" {
		invitationCodeStr := *req.InvitationCode

		// 招待者のIDを取得
		var inviterID string
		err = tx.GetContext(ctx, &inviterID, "SELECT id FROM users WHERE invitation_code = ?", invitationCodeStr)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// 招待コードの使用回数をチェック
		var couponCount int
		err = tx.GetContext(ctx, &couponCount, "SELECT COUNT(*) FROM coupons WHERE code = CONCAT('INV_', ?) FOR UPDATE", invitationCodeStr)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if couponCount >= 3 {
			writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
			return
		}

		// 招待クーポンと招待者へのリワードクーポンを一括挿入
		invCode := "INV_" + invitationCodeStr
		rewardCode := "RWD_" + invitationCodeStr + "_" + strconv.FormatInt(time.Now().UnixMilli(), 10)
		_, err = tx.ExecContext(ctx, `
			INSERT INTO coupons (user_id, code, discount) VALUES 
			(?, ?, 1500), 
			(?, ?, 1000)
		`, userID, invCode, inviterID, rewardCode)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "app_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &appPostUsersResponse{
		ID:             userID,
		InvitationCode: invitationCode,
	})
}

func appPostPaymentMethods(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostPaymentMethodsRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, errors.New("token is required but was empty"))
		return
	}

	user, ok := ctx.Value("user").(*User)
	if !ok {
		writeError(w, http.StatusUnauthorized, errors.New("unauthorized"))
		return
	}

	_, err := db.ExecContext(ctx, `
		INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)
	`, user.ID, req.Token)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func appGetRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user, ok := ctx.Value("user").(*User)
	if !ok {
		writeError(w, http.StatusUnauthorized, errors.New("unauthorized"))
		return
	}

	// ジョインを用いた最適化クエリ
	query := `
		SELECT 
			r.id, r.pickup_latitude, r.pickup_longitude, 
			r.destination_latitude, r.destination_longitude, 
			r.evaluation, r.created_at, r.updated_at,
			MAX(rs.created_at) AS latest_status_time,
			MAX(CASE WHEN rs.created_at = (SELECT MAX(created_at) FROM ride_statuses WHERE ride_id = r.id) THEN rs.status ELSE NULL END) AS status,
			c.id AS chair_id, c.owner_id, c.name AS chair_name, c.model AS chair_model,
			o.name AS owner_name
		FROM rides r
		LEFT JOIN ride_statuses rs ON r.id = rs.ride_id
		LEFT JOIN chairs c ON r.chair_id = c.id
		LEFT JOIN owners o ON c.owner_id = o.id
		WHERE r.user_id = ?
		GROUP BY r.id
		ORDER BY r.created_at DESC
	`

	type RideData struct {
		ID                   string         `db:"id"`
		PickupLatitude       int            `db:"pickup_latitude"`
		PickupLongitude      int            `db:"pickup_longitude"`
		DestinationLatitude  int            `db:"destination_latitude"`
		DestinationLongitude int            `db:"destination_longitude"`
		Evaluation           *int           `db:"evaluation"`
		CreatedAt            time.Time      `db:"created_at"`
		UpdatedAt            time.Time      `db:"updated_at"`
		LatestStatusTime     time.Time      `db:"latest_status_time"`
		Status               sql.NullString `db:"status"`
		ChairID              string         `db:"chair_id"`
		ChairOwnerID         string         `db:"owner_id"`
		ChairName            string         `db:"chair_name"`
		ChairModel           string         `db:"chair_model"`
		OwnerName            string         `db:"owner_name"`
	}

	var ridesData []RideData
	err := db.SelectContext(ctx, &ridesData, query, user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	items := make([]getAppRidesResponseItem, 0, len(ridesData))
	for _, rd := range ridesData {
		if rd.Status.String != "COMPLETED" {
			continue
		}

		// トランザクション内で計算する必要がないため、dbを渡す
		fare, err := calculateDiscountedFare(ctx, db, user.ID, &Ride{
			ID:                   rd.ID,
			PickupLatitude:       rd.PickupLatitude,
			PickupLongitude:      rd.PickupLongitude,
			DestinationLatitude:  rd.DestinationLatitude,
			DestinationLongitude: rd.DestinationLongitude,
			Evaluation:           rd.Evaluation,
		}, rd.PickupLatitude, rd.PickupLongitude, rd.DestinationLatitude, rd.DestinationLongitude)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		item := getAppRidesResponseItem{
			ID:                    rd.ID,
			PickupCoordinate:      Coordinate{Latitude: rd.PickupLatitude, Longitude: rd.PickupLongitude},
			DestinationCoordinate: Coordinate{Latitude: rd.DestinationLatitude, Longitude: rd.DestinationLongitude},
			Fare:                  fare,
			Evaluation:            getIntValue(rd.Evaluation),
			RequestedAt:           rd.CreatedAt.UnixMilli(),
			CompletedAt:           rd.UpdatedAt.UnixMilli(),
			Chair: getAppRidesResponseItemChair{
				ID:    rd.ChairID,
				Owner: rd.OwnerName,
				Name:  rd.ChairName,
				Model: rd.ChairModel,
			},
		}

		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, &getAppRidesResponse{
		Rides: items,
	})
}

func appPostRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user, ok := ctx.Value("user").(*User)
	if !ok {
		writeError(w, http.StatusUnauthorized, errors.New("unauthorized"))
		return
	}

	rideID := ulid.Make().String()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// 進行中のライドのカウントを効率的に取得
	var ongoingRides int
	err = tx.GetContext(ctx, &ongoingRides, `
		SELECT COUNT(*) 
		FROM rides r
		JOIN ride_statuses rs ON r.id = rs.ride_id
		WHERE r.user_id = ? AND rs.status != 'COMPLETED'
	`, user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if ongoingRides > 0 {
		writeError(w, http.StatusConflict, errors.New("ride already exists"))
		return
	}

	// 新しいライドと初期ステータスの一括挿入
	statusID := ulid.Make().String()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude)
		VALUES (?, ?, ?, ?, ?, ?);
		INSERT INTO ride_statuses (id, ride_id, status)
		VALUES (?, ?, 'MATCHING');
	`, rideID, user.ID, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude, statusID, rideID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ライドカウントの取得
	var rideCount int
	err = tx.GetContext(ctx, &rideCount, `SELECT COUNT(*) FROM rides WHERE user_id = ?`, user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// クーポンの使用
	if rideCount == 1 {
		// 初回利用クーポンの優先使用
		var coupon Coupon
		err = tx.GetContext(ctx, &coupon, `
			SELECT * FROM coupons 
			WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL 
			FOR UPDATE
		`, user.ID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if err == nil {
			// 'CP_NEW2024' クーポンを使用
			_, err = tx.ExecContext(ctx, `
				UPDATE coupons SET used_by = ? WHERE id = ?
			`, rideID, coupon.ID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			// 他の未使用クーポンを使用
			err = tx.GetContext(ctx, &coupon, `
				SELECT * FROM coupons 
				WHERE user_id = ? AND used_by IS NULL 
				ORDER BY created_at LIMIT 1 
				FOR UPDATE
			`, user.ID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			if err == nil {
				_, err = tx.ExecContext(ctx, `
					UPDATE coupons SET used_by = ? WHERE id = ?
				`, rideID, coupon.ID)
				if err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}
		}
	} else {
		// 他のライド: 未使用クーポンを順番に使用
		var coupon Coupon
		err = tx.GetContext(ctx, &coupon, `
			SELECT * FROM coupons 
			WHERE user_id = ? AND used_by IS NULL 
			ORDER BY created_at LIMIT 1 
			FOR UPDATE
		`, user.ID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `
				UPDATE coupons SET used_by = ? WHERE id = ?
			`, rideID, coupon.ID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	}

	// 料金計算
	fare, err := calculateDiscountedFare(ctx, tx, user.ID, &Ride{
		ID:                   rideID,
		PickupLatitude:       req.PickupCoordinate.Latitude,
		PickupLongitude:      req.PickupCoordinate.Longitude,
		DestinationLatitude:  req.DestinationCoordinate.Latitude,
		DestinationLongitude: req.DestinationCoordinate.Longitude,
		Evaluation:           nil,
	}, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusAccepted, &appPostRidesResponse{
		RideID: rideID,
		Fare:   fare,
	})
}

func appPostRidesEstimatedFare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesEstimatedFareRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user, ok := ctx.Value("user").(*User)
	if !ok {
		writeError(w, http.StatusUnauthorized, errors.New("unauthorized"))
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	discounted, err := calculateDiscountedFare(ctx, tx, user.ID, nil, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRidesEstimatedFareResponse{
		Fare:     discounted,
		Discount: calculateFare(req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude) - discounted,
	})
}

type appPostRidesEstimatedFareRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesEstimatedFareResponse struct {
	Fare     int `json:"fare"`
	Discount int `json:"discount"`
}

func appPostRideEvaluatation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.URL.Query().Get("ride_id") // r.PathValue は標準では存在しないためURLクエリパラメータを想定

	req := &appPostRideEvaluationRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Evaluation < 1 || req.Evaluation > 5 {
		writeError(w, http.StatusBadRequest, errors.New("evaluation must be between 1 and 5"))
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// ライドと最新ステータスを一括取得
	query := `
		SELECT 
			r.id, r.user_id, r.evaluation, 
			MAX(rs.created_at) AS latest_status_time,
			MAX(CASE WHEN rs.created_at = (SELECT MAX(created_at) FROM ride_statuses WHERE ride_id = r.id) THEN rs.status ELSE NULL END) AS latest_status
		FROM rides r
		LEFT JOIN ride_statuses rs ON r.id = rs.ride_id
		WHERE r.id = ?
		GROUP BY r.id
	`
	type RideInfo struct {
		ID           string         `db:"id"`
		UserID       string         `db:"user_id"`
		Evaluation   *int           `db:"evaluation"`
		LatestStatus sql.NullString `db:"latest_status"`
	}

	var rideInfo RideInfo
	err = tx.GetContext(ctx, &rideInfo, query, rideID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if rideInfo.LatestStatus.String != "ARRIVED" {
		writeError(w, http.StatusBadRequest, errors.New("not arrived yet"))
		return
	}

	// 評価を更新し、COMPLETEDステータスを挿入
	completedStatusID := ulid.Make().String()
	_, err = tx.ExecContext(ctx, `
		UPDATE rides SET evaluation = ? WHERE id = ?;
		INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, 'COMPLETED');
	`, req.Evaluation, rideID, completedStatusID, rideID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 支払いトークンの取得
	var paymentToken PaymentToken
	err = tx.GetContext(ctx, &paymentToken, `
		SELECT * FROM payment_tokens WHERE user_id = ?
	`, rideInfo.UserID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusBadRequest, errors.New("payment token not registered"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 料金計算
	// ライド情報が不完全なため、必要なフィールドを取得
	var ride Ride
	err = tx.GetContext(ctx, &ride, `
		SELECT * FROM rides WHERE id = ?
	`, rideID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, ride.UserID, &ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	fmt.Println(fare)

	// 支払いゲートウェイURLの取得
	var paymentGatewayURL string
	err = tx.GetContext(ctx, &paymentGatewayURL, `
		SELECT value FROM settings WHERE name = 'payment_gateway_url'
	`)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ライドの更新時刻を取得
	err = db.GetContext(ctx, &ride.UpdatedAt, `
		SELECT updated_at FROM rides WHERE id = ?
	`, rideID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRideEvaluationResponse{
		CompletedAt: ride.UpdatedAt.UnixMilli(),
	})
}

func appGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user, ok := ctx.Value("user").(*User)
	if !ok {
		writeError(w, http.StatusUnauthorized, errors.New("unauthorized"))
		return
	}

	// 最新のライドと未送信のステータスを一括取得
	query := `
		SELECT 
			r.id, r.pickup_latitude, r.pickup_longitude, 
			r.destination_latitude, r.destination_longitude, 
			r.fare, r.status, r.chair_id, r.created_at, r.updated_at,
			rs.id AS status_id, rs.status AS status_status
		FROM rides r
		LEFT JOIN ride_statuses rs ON r.id = rs.ride_id AND rs.app_sent_at IS NULL
		WHERE r.user_id = ?
		ORDER BY r.created_at DESC LIMIT 1
	`

	type NotificationRide struct {
		ID                   string         `db:"id"`
		PickupLatitude       int            `db:"pickup_latitude"`
		PickupLongitude      int            `db:"pickup_longitude"`
		DestinationLatitude  int            `db:"destination_latitude"`
		DestinationLongitude int            `db:"destination_longitude"`
		Fare                 int            `db:"fare"`
		Status               string         `db:"status"`
		ChairID              string         `db:"chair_id"`
		CreatedAt            time.Time      `db:"created_at"`
		UpdatedAt            time.Time      `db:"updated_at"`
		StatusID             sql.NullString `db:"status_id"`
		StatusStatus         sql.NullString `db:"status_status"`
	}

	var ride NotificationRide
	err := db.GetContext(ctx, &ride, query, user.ID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusOK, &appGetNotificationResponse{
				RetryAfterMs: 30,
			})
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ステータスの決定
	status := ride.Status
	if ride.StatusID.Valid {
		status = ride.StatusStatus.String
	}

	// 料金計算
	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	fare, err := calculateDiscountedFare(ctx, tx, user.ID, &Ride{
		ID:                   ride.ID,
		PickupLatitude:       ride.PickupLatitude,
		PickupLongitude:      ride.PickupLongitude,
		DestinationLatitude:  ride.DestinationLatitude,
		DestinationLongitude: ride.DestinationLongitude,
		Evaluation:           nil,
	}, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	response := &appGetNotificationResponse{
		Data: &appGetNotificationResponseData{
			RideID: ride.ID,
			PickupCoordinate: Coordinate{
				Latitude:  ride.PickupLatitude,
				Longitude: ride.PickupLongitude,
			},
			DestinationCoordinate: Coordinate{
				Latitude:  ride.DestinationLatitude,
				Longitude: ride.DestinationLongitude,
			},
			Fare:      fare,
			Status:    status,
			CreatedAt: ride.CreatedAt.UnixMilli(),
			UpdateAt:  ride.UpdatedAt.UnixMilli(),
		},
		RetryAfterMs: 30,
	}

	// チェアの詳細を取得
	if ride.ChairID != "" {
		chairQuery := `
			SELECT 
				c.id, c.name, c.model,
				COUNT(r.id) AS total_rides_count, 
				AVG(r.evaluation) AS total_evaluation_avg
			FROM chairs c
			LEFT JOIN rides r ON c.id = r.chair_id AND r.evaluation IS NOT NULL
			WHERE c.id = ?
			GROUP BY c.id
		`
		type ChairStats struct {
			ID                 string  `db:"id"`
			Name               string  `db:"name"`
			Model              string  `db:"model"`
			TotalRidesCount    int     `db:"total_rides_count"`
			TotalEvaluationAvg float64 `db:"total_evaluation_avg"`
		}

		var chair ChairStats
		err = tx.GetContext(ctx, &chair, chairQuery, ride.ChairID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		response.Data.Chair = &appGetNotificationResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			Stats: appGetNotificationResponseChairStats{
				TotalRidesCount:    chair.TotalRidesCount,
				TotalEvaluationAvg: chair.TotalEvaluationAvg,
			},
		}
	}

	// 未送信のステータスが存在する場合、app_sent_atを更新
	if ride.StatusID.Valid {
		_, err = tx.ExecContext(ctx, `
			UPDATE ride_statuses SET app_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?
		`, ride.StatusID.String)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, response)
}

func appGetNearbyChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	distanceStr := r.URL.Query().Get("distance")
	if latStr == "" || lonStr == "" {
		writeError(w, http.StatusBadRequest, errors.New("latitude or longitude is empty"))
		return
	}

	lat, err := strconv.Atoi(latStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("latitude is invalid"))
		return
	}

	lon, err := strconv.Atoi(lonStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("longitude is invalid"))
		return
	}

	distance := 50
	if distanceStr != "" {
		distance, err = strconv.Atoi(distanceStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("distance is invalid"))
			return
		}
	}

	coordinate := Coordinate{Latitude: lat, Longitude: lon}

	// ジョインを用いた最適化クエリ
	query := `
		SELECT 
			c.id, c.name, c.model, cl.latitude, cl.longitude
		FROM chairs c
		JOIN chair_locations cl ON c.id = cl.chair_id
		LEFT JOIN (
			SELECT r.chair_id
			FROM rides r
			JOIN ride_statuses rs ON r.id = rs.ride_id
			WHERE rs.status != 'COMPLETED'
			GROUP BY r.chair_id
		) ongoing ON c.id = ongoing.chair_id
		WHERE 
			c.is_active = TRUE 
			AND ongoing.chair_id IS NULL
			AND (ABS(cl.latitude - ?) + ABS(cl.longitude - ?)) <= ?
	`

	var chairs []struct {
		ID        string `db:"id"`
		Name      string `db:"name"`
		Model     string `db:"model"`
		Latitude  int    `db:"latitude"`
		Longitude int    `db:"longitude"`
	}
	err = db.SelectContext(ctx, &chairs, query, coordinate.Latitude, coordinate.Longitude, distance)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	nearbyChairs := make([]appGetNearbyChairsResponseChair, 0, len(chairs))
	for _, c := range chairs {
		nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{
			ID:    c.ID,
			Name:  c.Name,
			Model: c.Model,
			CurrentCoordinate: Coordinate{
				Latitude:  c.Latitude,
				Longitude: c.Longitude,
			},
		})
	}

	// 現在のタイムスタンプを取得
	retrievedAt := time.Now().UnixMilli()

	writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
		Chairs:      nearbyChairs,
		RetrievedAt: retrievedAt,
	})
}

// Helper Functions

func getIntValue(i *int) int {
	if i != nil {
		return *i
	}
	return 0
}

func calculateDistance(aLatitude, aLongitude, bLatitude, bLongitude int) int {
	return abs(aLatitude-bLatitude) + abs(aLongitude-bLongitude)
}

func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

func calculateFare(pickupLatitude, pickupLongitude, destLatitude, destLongitude int) int {
	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	return initialFare + meteredFare
}

func calculateDiscountedFare(ctx context.Context, dbInterface interface{}, userID string, ride *Ride, pickupLatitude, pickupLongitude, destLatitude, destLongitude int) (int, error) {
	var discount int

	switch dbConn := dbInterface.(type) {
	case *sqlx.Tx:
		// クーポンの取得
		var coupon Coupon
		if ride != nil {
			// 既存のライドに紐づくクーポン
			err := dbConn.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", ride.ID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
			if err == nil {
				discount = coupon.Discount
			}
		} else {
			// 新規ライド: 'CP_NEW2024' クーポンを優先的に使用
			err := dbConn.GetContext(ctx, &coupon, `
				SELECT * FROM coupons 
				WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL 
				ORDER BY created_at LIMIT 1
			`, userID)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
				// 他の未使用クーポンを使用
				err = dbConn.GetContext(ctx, &coupon, `
					SELECT * FROM coupons 
					WHERE user_id = ? AND used_by IS NULL 
					ORDER BY created_at LIMIT 1
				`, userID)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
				if err == nil {
					discount = coupon.Discount
				}
			} else {
				discount = coupon.Discount
			}
		}

		// 料金計算
		meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
		discountedMeteredFare := meteredFare - discount
		if discountedMeteredFare < 0 {
			discountedMeteredFare = 0
		}
		totalFare := initialFare + discountedMeteredFare

		return totalFare, nil

	case *sqlx.DB:
		// クーポンの取得
		var coupon Coupon
		if ride != nil {
			// 既存のライドに紐づくクーポン
			err := dbConn.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", ride.ID)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
			if err == nil {
				discount = coupon.Discount
			}
		} else {
			// 新規ライド: 'CP_NEW2024' クーポンを優先的に使用
			err := dbConn.GetContext(ctx, &coupon, `
				SELECT * FROM coupons 
				WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL 
				ORDER BY created_at LIMIT 1
			`, userID)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
				// 他の未使用クーポンを使用
				err = dbConn.GetContext(ctx, &coupon, `
					SELECT * FROM coupons 
					WHERE user_id = ? AND used_by IS NULL 
					ORDER BY created_at LIMIT 1
				`, userID)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
				if err == nil {
					discount = coupon.Discount
				}
			} else {
				discount = coupon.Discount
			}
		}

		// 料金計算
		meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
		discountedMeteredFare := meteredFare - discount
		if discountedMeteredFare < 0 {
			discountedMeteredFare = 0
		}
		totalFare := initialFare + discountedMeteredFare

		return totalFare, nil

	default:
		return 0, errors.New("invalid database interface")
	}
}
