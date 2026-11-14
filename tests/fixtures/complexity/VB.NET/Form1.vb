Imports System.Windows.Forms

Public Class Form1
    Implements IDisposable

    Private ReadOnly service As New CustomerService()

    Public Sub New()
        InitializeComponent()
        AddHandler NameBox.TextChanged, AddressOf OnNameChanged
    End Sub

#Region "Handlers"
    Private Sub SaveButton_Click(sender As Object, e As EventArgs) Handles SaveButton.Click
        service.Save(NameBox.Text)
        UpdateTitle()
    End Sub

    Private Sub OnNameChanged(sender As Object, e As EventArgs)
        UPDATETITLE()
    End Sub
#End Region

    Private Sub UpdateTitle()
        Me.Text = service.Describe()
    End Sub
End Class
